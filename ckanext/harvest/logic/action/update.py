import hashlib

import pylons.config as config
import logging
import datetime
import json
from sqlalchemy import or_

import ckan.plugins.toolkit as t
from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import PluginImplementations
from ckan.logic import get_action, get_or_bust
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.search.common import SearchIndexError, make_connection
from ckan.lib.search import clear as clear_package

from ckan.model import Package

from ckan.logic import NotFound, ValidationError, check_access
from ckan.lib.navl.dictization_functions import validate

from ckanext.harvest.queue import get_gather_publisher, resubmit_jobs

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.logic.schema import default_harvest_source_schema
from ckanext.harvest.logic.dictization import harvest_source_dictize, harvest_job_dictize

from ckanext.harvest.logic.action.create import _error_summary
from ckanext.harvest.logic.action.get import harvest_source_show, harvest_job_list, get_sources
from ckanext.harvest import lib as harvest_lib


log = logging.getLogger(__name__)

def harvest_source_update(context,data_dict):

    check_access('harvest_source_update',context,data_dict)

    model = context['model']
    session = context['session']

    source_name_or_id = data_dict.get('id') or data_dict.get('name')
    schema = context.get('schema') or default_harvest_source_schema()

    log.info('Harvest source %s update: %r', source_name_or_id, data_dict)
    source = HarvestSource.by_name_or_id(source_name_or_id)
    if not source:
        log.error('Harvest source %s does not exist', source_name_or_id)
        raise NotFound('Harvest source %s does not exist' % source_name_or_id)
    data_dict['id'] = source.id

    data, errors = validate(data_dict, schema, context=context)

    if errors:
        session.rollback()
        raise ValidationError(errors,_error_summary(errors))

    fields = ['url','title','type','description','user_id','publisher_id','frequency','name']
    for f in fields:
        if f in data and data[f] is not None:
            if f == 'url':
                data[f] = data[f].strip()
            source.__setattr__(f,data[f])

    if 'active' in data_dict:
        source.active = data['active']

    if 'config' in data_dict:
        source.config = data['config']

    source.save()
    # Abort any pending jobs
    if not source.active:
        jobs = HarvestJob.filter(source=source,status=u'New')
        log.info('Harvest source %s not active, so aborting %i outstanding jobs', source_name_or_id, jobs.count())
        if jobs:
            for job in jobs:
                job.status = u'Aborted'
                job.save()

    # Ensure sqlalchemy writes to the db immediately, since the gather/fetch
    # runs in a different process and needs the latest source info. Not sure if
    # this works, but try it.
    model.repo.commit_and_remove()

    return harvest_source_dictize(source,context)

def harvest_source_clear(context, data_dict):
    '''
    Clears all datasets, jobs and objects related to a harvest source, but keeps the source itself.
    This is useful to clean history of long running harvest sources to start again fresh.

    :param id: the id of the harvest source to clear
    :type id: string

    '''
    check_access('harvest_source_clear',context,data_dict)

    harvest_source_id = data_dict.get('id',None)

    source = HarvestSource.by_name_or_id(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    model = context['model']

    # we want to call clear_package on every item we know about

    lookup = "select package_id from harvest_object where harvest_source_id='{harvest_source_id}'"\
        .format(harvest_source_id=harvest_source_id)
    ids = [row[0] for row in model.Session.execute(lookup)]
    if ids:
        log.info('Removing Harvest source datasets from solr before deletion: %s', harvest_source_id)

    for pid in ids:
        if pid:
            clear_package(pid)


    sql = "select id from related where id in (select related_id from related_dataset where dataset_id in (select package_id from harvest_object where harvest_source_id = '{harvest_source_id}'));".format(harvest_source_id=harvest_source_id)
    result = model.Session.execute(sql)
    ids = []
    for row in result:
        ids.append(row[0])
    related_ids = "('" + "','".join(ids) + "')"

    sql = '''begin;
    update package set state = 'to_delete' where id in (select package_id from harvest_object where harvest_source_id = '{harvest_source_id}');'''.format(
        harvest_source_id=harvest_source_id)

    # CKAN-2.3 or above: delete resource views, resource revisions & resources
    if t.check_ckan_version(min_version='2.3'):
        sql += '''
        delete from resource_view where resource_id in (select id from resource where package_id in (select id from package where state = 'to_delete' ));
        delete from resource_revision where package_id in (select id from package where state = 'to_delete' );
        delete from resource where package_id in (select id from package where state = 'to_delete' );
        '''
    # Backwards-compatibility: support ResourceGroup (pre-CKAN-2.3)
    else:
        sql += '''
        delete from resource_revision where resource_group_id in
        (select id from resource_group where package_id in
        (select id from package where state = 'to_delete'));
        delete from resource where resource_group_id in
        (select id from resource_group where package_id in
        (select id from package where state = 'to_delete'));
        delete from resource_group_revision where package_id in
        (select id from package where state = 'to_delete');
        delete from resource_group where package_id  in
        (select id from package where state = 'to_delete');
        '''
    sql += '''
    delete from harvest_coupled_resource where service_record_package_id in  (select id from package where state = 'to_delete');
    delete from harvest_coupled_resource where dataset_record_package_id in  (select id from package where state = 'to_delete');
    delete from harvest_object_error where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_extra where harvest_object_id in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    delete from harvest_gather_error where harvest_job_id in (select id from harvest_job where source_id = '{harvest_source_id}');
    delete from harvest_job where source_id = '{harvest_source_id}';
    delete from package_role where package_id in (select id from package where state = 'to_delete' );
    delete from user_object_role where id not in (select user_object_role_id from package_role) and context = 'Package';
    delete from package_tag_revision where package_id in (select id from package where state = 'to_delete');
    delete from member_revision where table_id in (select id from package where state = 'to_delete');
    delete from package_extra_revision where package_id in (select id from package where state = 'to_delete');
    delete from package_revision where id in (select id from package where state = 'to_delete');
    delete from package_tag where package_id in (select id from package where state = 'to_delete');
    delete from package_extra where package_id in (select id from package where state = 'to_delete');
    delete from package_relationship_revision where subject_package_id in (select id from package where state = 'to_delete');
    delete from package_relationship_revision where object_package_id in (select id from package where state = 'to_delete');
    delete from package_relationship where subject_package_id in (select id from package where state = 'to_delete');
    delete from package_relationship where object_package_id in (select id from package where state = 'to_delete');
    delete from member where table_id in (select id from package where state = 'to_delete');
    delete from related_dataset where dataset_id in (select id from package where state = 'to_delete');
    delete from related where id in {related_ids};
    delete from package where id in (select id from package where state = 'to_delete');
    commit;
    '''.format(
        harvest_source_id=harvest_source_id, related_ids=related_ids)

    log.info('Removing Harvest source datasets from database: %s', harvest_source_id)
    model.Session.execute(sql)
    log.debug('Datasets all removed')

    return {'id': harvest_source_id}


def harvest_objects_import(context,data_dict):
    '''
        Reimports the current harvest objects
        It performs the import stage with the last fetched objects, optionally
        belonging to a certain source.
        Please note that no objects will be fetched from the remote server.
        It will only affect the last fetched objects already present in the
        database.
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import',context,data_dict)

    model = context['model']
    session = context['session']
    # source_id param accepted for backwards API compatibility
    source_name_or_id = data_dict.get('source') or data_dict.get('source_id')
    guid = data_dict.get('guid',None)
    harvest_object_id = data_dict.get('harvest_object_id',None)
    package_id_or_name = data_dict.get('package_id',None)

    segments = context.get('segments',None)

    join_datasets = context.get('join_datasets',True)

    if guid:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.guid==guid) \
                .filter(HarvestObject.current==True)
    elif source_name_or_id:
        source = HarvestSource.by_name_or_id(source_name_or_id)
        if not source:
            log.error('Harvest source %s does not exist', source_name_or_id)
            raise NotFound('Harvest source %s does not exist' % source_name_or_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_name_or_id)
            raise Exception('This harvest source is not active')

        last_objects_ids = session.query(HarvestObject.id) \
                .join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True)

    elif harvest_object_id:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.id==harvest_object_id)
    elif package_id_or_name:
        last_objects_ids = session.query(HarvestObject.id) \
            .join(Package) \
            .filter(HarvestObject.current==True) \
            .filter(Package.state==u'active') \
            .filter(or_(Package.id==package_id_or_name,
                        Package.name==package_id_or_name))
        join_datasets = False
    else:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.current==True)

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state==u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0
    import_count = 0

    for obj_id in last_objects_ids:
        if segments and str(hashlib.md5(obj_id[0]).hexdigest())[0] not in segments:
            continue

        obj = session.query(HarvestObject).get(obj_id)

        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester,'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                import_count += 1
                break
        last_objects_count += 1
    log.info('Harvest objects imported: %s/%s', import_count,
             last_objects_count)
    return import_count

def _caluclate_next_run(frequency):

    now = datetime.datetime.utcnow()
    if frequency == 'ALWAYS':
        return now
    if frequency == 'DAILY':
        return now + datetime.timedelta(days=1)
    if frequency == 'WEEKLY':
        return now + datetime.timedelta(weeks=1)
    if frequency in ('BIWEEKLY', 'FORTNIGHTLY'):
        return now + datetime.timedelta(weeks=2)
    if frequency == 'MONTHLY':
        if now.month in (4,6,9,11):
            days = 30
        elif now.month == 2:
            if now.year % 4 == 0:
                days = 29
            else:
                days = 28
        else:
            days = 31
        return now + datetime.timedelta(days=days)
    raise Exception('Frequency {freq} not recognised'.format(freq=frequency))


def _make_scheduled_jobs(context, data_dict):

    data_dict = {'only_to_run': True,
                 'only_active': True,
                 'only_mine': True}
    sources = get_sources(context, data_dict)

    for source in sources:
        data_dict = {'source_id': source.id}
        try:
            get_action('harvest_job_create')(context, data_dict)
        except HarvestJobExists, e:
            log.info('Trying to rerun job for %s skipping' % source.id)

        source.next_run = _caluclate_next_run(source.frequency)
        source.save()

def harvest_jobs_run(context,data_dict):
    '''
    Runs any jobs that have been requested by a user or 'scheduled' for the
    current date/time.
    Checks running jobs to see if they are finished and if so, mark them as so.

    :returns: jobs that are now started
    :rtype: list of dictionaries
    '''
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run',context,data_dict)

    session = context['session']

    source_id = data_dict.get('source_id',None)

    if not source_id:
        _make_scheduled_jobs(context, data_dict)
    else:
        source = harvest_source_show(context, {'id': source_id})
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)
        source_id = source['id']

    context['return_objects'] = False

    # Flag finished jobs as such
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'Running'})
    if len(jobs):
        for job in jobs:
            if job['gather_finished']:
                harvest_lib.update_job_status(job, session)

    # resubmit old redis tasks
    resubmit_jobs()

    # Check if there are new (i.e. pending) harvest jobs
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'New'})
    log.info('Number of jobs: %i', len(jobs))
    sent_jobs = []
    if len(jobs) == 0:
        log.info('No new harvest jobs.')
        return sent_jobs # i.e. []
        # Do not raise an exception as that will cause cron (which runs
        # this) to produce an error email.

    # Send each new job to the gather queue
    publisher = get_gather_publisher()
    for job in jobs:
        context['include_status'] = False
        source = harvest_source_show(context,{'id':job['source']})
        if source['active']:
            job_obj = HarvestJob.get(job['id'])
            job_obj.status = job['status'] = u'Running'
            job_obj.save()
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()

    # Record the running in harvest_status
    log.info('%i jobs sent to the gather queue to be harvested', len(sent_jobs))

    return sent_jobs


def harvest_job_abort(context, data_dict):
    '''Aborts a harvest job. Given a harvest source_id, it looks for the latest
    one and (assuming it is not Finished) marks it as Aborted. It also marks
    any of that source's harvest objects and (if not complete or error) marks
    them "ABORTED", so any left in limbo are cleaned up. Does not actually stop
    running any queued harvest fetchs/objects.

    :param source_id: the name or id of the harvest source with a job to abort
    :type source_id: string
    '''

    check_access('harvest_job_abort', context, data_dict)

    model = context['model']

    source_id = data_dict.get('source_id', None)
    source = harvest_source_show(context, {'id': source_id})

    # HarvestJob set to 'Aborted'
    # Don not use harvest_job_list since it can use a lot of memory
    last_job = model.Session.query(HarvestJob) \
                    .filter_by(source_id=source['id']) \
                    .order_by(HarvestJob.created.desc()).first()
    if not last_job:
        raise NotFound('Error: source has no jobs')
    job = get_action('harvest_job_show')(context,
                                         {'id': last_job.id})

    if job['status'] not in ('Finished', 'Aborted'):
        # i.e. New or Running
        job_obj = HarvestJob.get(job['id'])
        job_obj.status = new_status = 'Aborted'
        model.repo.commit_and_remove()
        log.info('Harvest job changed status from "%s" to "%s"',
                 job['status'], new_status)
    else:
        log.info('Harvest job unchanged. Source %s status is: "%s"',
                 job['id'], job['status'])

    # HarvestObjects set to ABORTED
    job_obj = HarvestJob.get(job['id'])
    objs = job_obj.objects
    for obj in objs:
        if obj.state not in ('COMPLETE', 'ERROR', 'ABORTED'):
            old_state = obj.state
            obj.state = 'ABORTED'
            log.info('Harvest object changed state from "%s" to "%s": %s',
                     old_state, obj.state, obj.id)
        else:
            log.info('Harvest object not changed from "%s": %s',
                     obj.state, obj.id)
    model.repo.commit_and_remove()

    job_obj = HarvestJob.get(job['id'])
    return harvest_job_dictize(job_obj, context)



def harvest_source_reindex(context, data_dict):
    '''Reindex a single harvest source'''

    harvest_source_id = get_or_bust(data_dict, 'id')
    defer_commit = False #context.get('defer_commit', False)

    if 'extras_as_string'in context:
        del context['extras_as_string']
    context.update({'ignore_auth': True})
    package_dict = get_action('harvest_source_show')(context,
        {'id': harvest_source_id})
    log.debug('Updating search index for harvest source {0}'.format(harvest_source_id))

    # Remove configuration values
    new_dict = {}
    if package_dict.get('config'):
        config = json.loads(package_dict['config'])
        for key, value in package_dict.iteritems():
            if key not in config:
                new_dict[key] = value
    package_index = PackageSearchIndex()
    package_index.index_package(new_dict, defer_commit=defer_commit)

    return True

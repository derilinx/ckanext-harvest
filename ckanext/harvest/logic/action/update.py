import hashlib

import logging
import datetime
from sqlalchemy import or_

from ckan.plugins import PluginImplementations
from ckan.logic import get_action
from ckanext.harvest.interfaces import IHarvester

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
    source_id = data_dict.get('source_id',None)
    guid = data_dict.get('guid',None)
    harvest_object_id = data_dict.get('harvest_object_id',None)
    package_id_or_name = data_dict.get('package_id',None)

    segments = context.get('segments',None)

    join_datasets = context.get('join_datasets',True)

    if guid:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.guid==guid) \
                .filter(HarvestObject.current==True)
    elif source_id:
        source = HarvestSource.get(source_id)
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_id)
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

    check_access('harvest_job_abort', context, data_dict)

    model = context['model']

    source_id = data_dict.get('source_id', None)
    jobs = get_action('harvest_job_list')(context,
                                          {'source_id': source_id})
    if not jobs:
        raise NotFound('Error: source has no jobs')
    job = jobs[0]  # latest one

    if job['status'] not in ('Finished', 'Aborted'):
        job_obj = HarvestJob.get(job['id'])
        job_obj.status = new_status = 'Aborted'
        model.repo.commit_and_remove()
        log.info('Changed the harvest job status to "{0}"'.format(new_status))
    else:
        log.info('Nothing to do')

    job_obj = HarvestJob.get(job['id'])
    return harvest_job_dictize(job_obj, context)

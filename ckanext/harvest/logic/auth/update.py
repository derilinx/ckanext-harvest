from ckan.plugins import toolkit as pt
from ckanext.harvest.logic.auth import user_is_sysadmin


def harvest_source_update(context, data_dict):
    '''
        Authorization check for harvest source update

        sysadmins only  *dgi3
    '''

    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can update harvest sources')}
    else:
        return {'success': True}


def harvest_sources_clear(context, data_dict):
    '''
        Authorization check for clearing history for all harvest sources

        Only sysadmins can do it
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can clear history for all harvest jobs')}
    else:
        return {'success': True}


def harvest_source_clear(context, data_dict):
    '''
        Authorization check for clearing a harvest source

        It forwards to harvest_source_update
    '''
    return harvest_source_update(context, data_dict)


def harvest_objects_import(context, data_dict):
    '''
        Authorization check reimporting all harvest objects

        Only sysadmins can do it
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can reimport all harvest objects')}
    else:
        return {'success': True}


def harvest_jobs_run(context, data_dict):
    '''
        Authorization check for running the pending harvest jobs

        Only sysadmins can do it
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can run the pending harvest jobs')}
    else:
        return {'success': True}


def harvest_send_job_to_gather_queue(context, data_dict):
    '''
        Authorization check for sending a job to the gather queue

        It forwards the checks to harvest_job_create, ie the user can only run
        the job if she is allowed to create the job.
    '''
    from ckanext.harvest.logic.auth.create import harvest_job_create
    return harvest_job_create(context, data_dict)


def harvest_job_abort(context, data_dict):
    '''
        Authorization check for aborting a running harvest job

        Same permissions as running one
    '''
    return harvest_jobs_run(context, data_dict)


def harvest_sources_reindex(context, data_dict):
    '''
        Authorization check for reindexing all harvest sources

        Only sysadmins can do it
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can reindex all harvest sources')}
    else:
        return {'success': True}


def harvest_source_reindex(context, data_dict):
    '''
        Authorization check for reindexing a harvest source

        It forwards to harvest_source_update
    '''
    return harvest_source_update(context, data_dict)

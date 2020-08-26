from ckan.plugins import toolkit as pt
from ckanext.harvest.logic.auth import user_is_sysadmin


def harvest_source_create(context, data_dict):
    '''
        Authorization check for harvest source creation

        Sysadmins only     *dgi3
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can create harvest sources')}
    else:
        return {'success': True}


def harvest_job_create(context, data_dict):
    '''
        Authorization check for harvest job creation

        Sysadmins only    *dgi3
    '''

    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can create harvest jobs')}
    else:
        return {'success': True}


def harvest_job_create_all(context, data_dict):
    '''
        Authorization check for creating new jobs for all sources

        Only sysadmins can do it
    '''
    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can create harvest jobs for all sources')}
    else:
        return {'success': True}


def harvest_object_create(context, data_dict):
    """
        Auth check for creating a harvest object

        only the sysadmins can create harvest objects
    """
    # sysadmins can run all actions if we've got to this point we're not a sysadmin
    return {'success': False, 'msg': pt._('Only the sysadmins can create harvest objects')}

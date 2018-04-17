from ckan.plugins import toolkit as pt


def harvest_source_delete(context, data_dict):
    '''
        Authorization check for harvest source deletion


        sysadmins only  *dgi3
    '''

    if not user_is_sysadmin(context):
        return {'success': False, 'msg': pt._('Only sysadmins can delete harvest sources')}
    else:
        return {'success': True}

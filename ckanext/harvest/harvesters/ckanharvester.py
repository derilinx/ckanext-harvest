import urllib
import urllib2
import httplib
import datetime
import socket

from sqlalchemy import exists

from ckan.lib.base import c
from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_name
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase

# we use this for parsing ISO dates
import arrow

org_blacklist = [
    '89a1c72e-fba7-4935-9575-956325cc03f6',  # nui maynooth airo
    'nui-maynooth-airo',
    'national-transport-authority'
]
dataset_blacklist = [
    'weather-stations'
]
dataset_whitelist = [
    'real-time-passenger-information-rtpi-for-dublin-bus-bus-eireann-luas-and-irish-rail'
]

hsebaseurl = 'http://172.104.140.57'

class CKANHarvester(HarvesterBase):
    '''
    A Harvester for CKAN instances
    '''
    config = None

    api_version = 2
    action_api_version = 3

    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version

    def _get_search_api_offset(self):
        return '%s/package_search' % self._get_action_api_offset()

    def _get_content(self, url):
        http_request = urllib2.Request(url=url)

        api_key = self.config.get('api_key')
        if api_key:
            http_request.add_header('Authorization', api_key)

        try:
            http_response = urllib2.urlopen(http_request)
        except urllib2.HTTPError, e:
            if e.getcode() == 404:
                raise ContentNotFoundError('HTTP error: %s' % e.code)
            else:
                raise ContentFetchError('HTTP error: %s' % e.code)
        except urllib2.URLError, e:
            raise ContentFetchError('URL error: %s' % e.reason)
        except httplib.HTTPException, e:
            raise ContentFetchError('HTTP Exception: %s' % e)
        except socket.error, e:
            raise ContentFetchError('HTTP socket error: %s' % e)
        except Exception, e:
            raise ContentFetchError('HTTP general exception: %s' % e)
        return http_response.read()

    def _get_group(self, base_url, group_name):
        url = base_url + self._get_action_api_offset() + '/group_show?id=' + \
            munge_name(group_name)
        try:
            content = self._get_content(url)
            return json.loads(content)
        except (ContentFetchError, ValueError):
            log.debug('Could not fetch/decode remote group')
            raise RemoteResourceError('Could not fetch/decode remote group')

    def _get_organization(self, base_url, org_name):
        url = base_url + self._get_action_api_offset() + \
            '/organization_show?id=' + org_name
        try:
            content = self._get_content(url)
            content_dict = json.loads(content)
            return content_dict['result']
        except (ContentFetchError, ValueError, KeyError):
            log.debug('Could not fetch/decode remote group')
            raise RemoteResourceError(
                'Could not fetch/decode remote organization')

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'ckan',
            'title': 'CKAN',
            'description': 'Harvests remote CKAN instances',
            'form_config_interface': 'Text'
        }

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'], list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model': model, 'user': c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(
                            context, {'id': group_name})
                    except NotFound, e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'], dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model': model, 'user': c.user}
                try:
                    user = get_action('user_show')(
                        context, {'id': config_obj.get('user')})
                except NotFound:
                    raise ValueError('User not found')

            for key in ('read_only', 'force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key], bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError, e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        log.debug('In CKANHarvester gather_stage (%s)',
                  harvest_job.source.url)
        toolkit.requires_ckan_version(min_version='2.0')
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_ckan_base_url = harvest_job.source.url.rstrip('/')

        # Filter in/out datasets from particular organizations
        fq_terms = []
        org_filter_include = self.config.get('organizations_filter_include', [])
        org_filter_exclude = self.config.get('organizations_filter_exclude', [])
        if org_filter_include:
            fq_terms.append(' OR '.join(
                'organization:%s' % org_name for org_name in org_filter_include))
        elif org_filter_exclude:
            fq_terms.extend(
                '-organization:%s' % org_name for org_name in org_filter_exclude)

        # Ideally we can request from the remote CKAN only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self._last_error_free_job(harvest_job)
        log.debug('Last error-free job: %r', last_error_free_job)
        #remove this after first harvest
        self.config['force_all'] = True
        if (last_error_free_job and
                not self.config.get('force_all', False)):
            get_all_packages = False

            # Request only the datasets modified since
            last_time = last_error_free_job.gather_started
            # Note: SOLR works in UTC, and gather_started is also UTC, so
            # this should work as long as local and remote clocks are
            # relatively accurate. Going back a little earlier, just in case.
            get_changes_since = \
                (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

            fq_since_last_time = 'metadata_modified:[{since}Z TO *]' \
                .format(since=get_changes_since)

            try:
                pkg_dicts = self._search_for_datasets(
                    remote_ckan_base_url,
                    fq_terms + [fq_since_last_time])
            except SearchError, e:
                log.info('Searching for datasets changed since last time '
                         'gave an error: %s', e)
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info('No datasets have been updated on the remote '
                         'CKAN instance since the last harvest job %s',
                         last_time)
                return None

        # Fall-back option - request all the datasets from the remote CKAN
        if get_all_packages:
            # Request all remote packages
            try:
                pkg_dicts = self._search_for_datasets(remote_ckan_base_url,
                                                      fq_terms)
            except SearchError, e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote CKAN for datasets:%s url:%s'
                    'terms:%s' % (e, remote_ckan_base_url, fq_terms),
                    harvest_job)
                return None
        if not pkg_dicts:
            self._save_gather_error(
                'No datasets found at CKAN: %s' % remote_ckan_base_url,
                harvest_job)
            return None


        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                filter(HarvestObject.current==True).\
                filter(HarvestObject.harvest_source_id==harvest_job.source.id)

        ###
        # ok so, HarvestObject.guid is the remote package_id,
        # and HarvestObject.package_id is the local package_id.
        ###
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())

        log.error("Number of datasets in db: %s" % len(guids_in_db))

        log.error(guids_in_db)

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []
            for pkg_dict in pkg_dicts:
                if pkg_dict['id'] in package_ids:
                    log.info('Discarding duplicate dataset %s - probably due '
                             'to datasets being changed at the same time as '
                             'when the harvester was paging through',
                             pkg_dict['id'])
                    continue
                package_ids.add(pkg_dict['id'])

            log.error(package_ids)

            to_delete = guids_in_db - package_ids
	    log.error("Number of datasets to delete: %s" % len(to_delete))

            for guid in to_delete:
                log.debug("Creating HarvestObject to delete %s %s", guid, guid_to_package_id[guid])
                obj = HarvestObject(guid=guid,
                                    job=harvest_job,
                                    extras=[HOExtra(key='status', value='delete')])
                model.Session.query(HarvestObject).\
                    filter_by(guid=guid).\
                    update({'current': False}, False)
                obj.save()
                object_ids.append(obj.id)

            for pkg_dict in pkg_dicts:
                log.debug('Creating HarvestObject for %s %s',
                          pkg_dict['name'], pkg_dict['id'])
                obj = HarvestObject(guid=pkg_dict['id'],
                                    job=harvest_job,
                                    extras=[HOExtra(key='status', value='update')],
                                    content=json.dumps(pkg_dict))
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)

    def _search_for_datasets(self, remote_ckan_base_url, fq_terms=None):
        '''Does a dataset search on a remote CKAN and returns the results.

        Deals with paging to return all the results, not just the first page.
        '''
        base_search_url = remote_ckan_base_url + self._get_search_api_offset()
        params = {'rows': '100', 'start': '0'}
        # There is the worry that datasets will be changed whilst we are paging
        # through them.
        # * In SOLR 4.7 there is a cursor, but not using that yet
        #   because few CKANs are running that version yet.
        # * However we sort, then new names added or removed before the current
        #   page would cause existing names on the next page to be missed or
        #   double counted.
        # * Another approach might be to sort by metadata_modified and always
        #   ask for changes since (and including) the date of the last item of
        #   the day before. However if the entire page is of the exact same
        #   time, then you end up in an infinite loop asking for the same page.
        # * We choose a balanced approach of sorting by ID, which means
        #   datasets are only missed if some are removed, which is far less
        #   likely than any being added. If some are missed then it is assumed
        #   they will harvested the next time anyway. When datasets are added,
        #   we are at risk of seeing datasets twice in the paging, so we detect
        #   and remove any duplicates.
        params['sort'] = 'id asc'
        if fq_terms:
            params['fq'] = ' '.join(fq_terms)

        pkg_dicts = []
        pkg_ids = set()
        previous_content = None
        while True:
            url = base_search_url + '?' + urllib.urlencode(params)
            log.debug('Searching for CKAN datasets: %s', url)
            try:
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError(
                    'Error sending request to search remote '
                    'CKAN instance %s using URL %r. Error: %s' %
                    (remote_ckan_base_url, url, e))

            if previous_content and content == previous_content:
                raise SearchError('The paging doesn\'t seem to work. URL: %s' %
                                  url)
            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote CKAN was not JSON: %r'
                                  % content)
            try:
                pkg_dicts_page = response_dict.get('result', {}).get('results',
                                                                     [])
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'result/results: %r' % response_dict)

            # Weed out any datasets found on previous pages (should datasets be
            # changing while we page)
            ids_in_page = set(p['id'] for p in pkg_dicts_page)
            duplicate_ids = ids_in_page & pkg_ids
            if duplicate_ids:
                pkg_dicts_page = [p for p in pkg_dicts_page
                                  if p['id'] not in duplicate_ids]
            pkg_ids |= ids_in_page

            pkg_dicts.extend(pkg_dicts_page)

            if len(pkg_dicts_page) == 0:
                break

            params['start'] = str(int(params['start']) + int(params['rows']))

        return pkg_dicts

    @classmethod
    def _last_error_free_job(cls, harvest_job):
        # TODO weed out cancelled jobs somehow.
        # look for jobs with no gather errors
        jobs = \
            model.Session.query(HarvestJob) \
                 .filter(HarvestJob.source == harvest_job.source) \
                 .filter(HarvestJob.gather_started != None) \
                 .filter(HarvestJob.status == 'Finished') \
                 .filter(HarvestJob.id != harvest_job.id) \
                 .filter(
                     ~exists().where(
                         HarvestGatherError.harvest_job_id == HarvestJob.id)) \
                 .order_by(HarvestJob.gather_started.desc())
        # now check them until we find one with no fetch/import errors
        # (looping rather than doing sql, in case there are lots of objects
        # and lots of jobs)
        for job in jobs:
            for obj in job.objects:
                if obj.current is False and \
                        obj.report_status != 'not modified':
                    # unsuccessful, so go onto the next job
                    break
            else:
                return job

    def fetch_stage(self, harvest_object):
        # Nothing to do here - we got the package dict in the search in the
        # gather stage
        return True

    def import_stage(self, harvest_object):
        log.debug('In CKANHarvester import_stage')

        context = {'model': model, 'session': model.Session,
                   'user': 'user_d1'}
                   # 'user': self._get_user_name()}
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        status = self._get_object_extra(harvest_object, 'status')

        log.debug(status)
        if status == 'delete':
            # Delete package
            context.update({
                'ignore_auth': True,
            })
            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.debug('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))

            return True

        try:
            package_dict = json.loads(harvest_object.content)

            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', 'only_local')
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                for group_name in package_dict['groups']:
                    #Some CKANs return full group dictionaries
                    if isinstance(group_name, dict):
                        group_name = group_name.get("id", "")
                    try:
                        data_dict = {'id': group_name}
                        group = get_action('group_show')(context, data_dict)
                        if self.api_version == 1:
                            validated_groups.append(group['name'])
                        else:
                            validated_groups.append(group['id'])
                    except NotFound, e:
                        log.info('Group %s is not available', group_name)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_name)
                            except RemoteResourceError:
                                log.error('Could not get remote group %s', group_name)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)

                            get_action('group_create')(context, group)
                            log.info('Group %s has been newly created', group_name)
                            if self.api_version == 1:
                                validated_groups.append(group['name'])
                            else:
                                validated_groups.append(group['id'])

                package_dict['groups'] = validated_groups

            # Local harvest source organization
            source_dataset = get_action('package_show')(context, {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', 'create')

            if not remote_orgs in ('only_local', 'create'):
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                if not 'owner_org' in package_dict:
                    package_dict['owner_org'] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict['organization']['name']

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(context, data_dict)
                        validated_org = org['id']
                        log.info('Got org %s', validated_org)
                    except NotFound, e:
                        log.info('Organization %s is not available', remote_org)
                        if remote_orgs == 'create' and not remote_org in org_blacklist:
                            try:
                                try:
                                    org = self._get_organization(harvest_object.source.url, remote_org)
                                except RemoteResourceError:
                                    # fallback if remote CKAN exposes organizations as groups
                                    # this especially targets older versions of CKAN
                                    org = self._get_group(harvest_object.source.url, remote_org)

                                for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                    org.pop(key, None)

                                org['contact-name']='-'
                                org['contact-email']='-'
                                org['contact-phone']='-'
                                get_action('organization_create')(context, org)
                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['name']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                if validated_org not in org_blacklist:
                    package_dict['owner_org'] = validated_org or local_org
                else:
		    self._save_object_error('Org in blacklist %s %s' % (package_dict['owner_org'], package_dict['name']), harvest_object, 'Import')
                    return False
                    

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []
                package_dict['groups'].extend(
                    [g for g in default_groups
                     if g not in package_dict['groups']])

            # Set default extras if needed
            default_extras = self.config.get('default_extras', {})
            def get_extra(key, package_dict):
                for extra in package_dict.get('extras', []):
                    if extra['key'] == key:
                        return extra
            if default_extras:
                override_extras = self.config.get('override_extras', False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key, value in default_extras.iteritems():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict['extras'].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip('/'),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict['id'])

                    package_dict['extras'].append({'key': key, 'value': value})

            for resource in package_dict.get('resources', []):
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                if (resource.get('url_type') == 'datastore' and resource.get('url')[0] == '/'):
                    # these are relative links
                    if (srcname == 'corkcity'):
                        resource['url'] = 'http://data.corkcity.ie' + resource.get('url')
                    elif (srcname == 'hse'):
                        resource['url'] = hsebaseurl + resource.get('url')

                resource.pop('url_type', None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop('revision_id', None)

                resource.pop('package_id', None)
                resource.pop('__extras', None);
                resource.pop('type', None)

            if len(package_dict.get('resources')) == 0:
                log.error('No resources for %s' % package_dict.get('name'))
                self._save_object_error('No resources for %s!' % (package_dict['owner_org'], package_dict['name']), harvest_object, 'Import')
                return False

            # we're doing our dublinked mapping here.
            #package_dict['tags'] = [t.get('name') for t in package_dict['tags']]

            if (package_dict['license_id'] == 'CC-BY-4.0'):
                package_dict['license_id'] = 'cc-by'

            name_health = "Health"

            theme_map = {
                "Transport and Infrastructure": "Transport",
                "Planning and Land Use": "Housing",
                "Environment and Energy": "Energy",
                "Government and Participation": "Government",
                "Recreation and Amenities": "Towns",
                "Population and Communities": "Society",
                "Arts Culture and Heritage": "Arts",
                "Public Health and Safety": name_health,
                "Economy and Innovation": "Economy",
            }

            srcname = harvest_object.job.source.title
            if (srcname == 'corkcity'):
                package_dict['collection-name'] = 'corkcity-ckan'
                package_dict['url'] = "https://data.corkcity.ie/dataset/%s" % package_dict['name']
                package_dict['title'] = package_dict['title']# + " - Cork"
            elif (srcname == 'dublinked'):
                package_dict['collection-name'] = 'dublinked-ckan'
                package_dict['url'] = "https://data.dublinked.ie/dataset/%s" % package_dict['name']
            elif (srcname == 'hse'):
                package_dict['collection-name'] = 'hse-ckan'
                package_dict['url'] = hsebaseurl + "/dataset/%s" % package_dict['name']
                #Set theme directly
                package_dict['theme-primary'] = name_health

            if ('category' in package_dict and package_dict['category'] in theme_map):
                package_dict['theme-primary'] = theme_map[package_dict['category']]
            elif ('category' in package_dict and package_dict['category'] in theme_map.values()):
                package_dict['theme-primary'] = package_dict['category']
            elif ('theme-primary' not in package_dict):
                package_dict['theme-primary'] = 'Towns'

            package_dict.pop('category', None)

            if not package_dict.get('language'):
                package_dict['language'] = 'eng' # they don't use this, so hardcode

            if (package_dict['owner_org'] in org_blacklist) and (package_dict['name'] not in dataset_whitelist):
                log.error('Org %s for dataset %s in blacklist!' % (package_dict['owner_org'], package_dict['name']))
                self._save_object_error('Org %s for dataset %s in blacklist!' % (package_dict['owner_org'], package_dict['name']), harvest_object, 'Import')
                return False

            if (package_dict['name'] in dataset_blacklist):
                self._save_object_error('dataset %s in blacklist!' % (package_dict['name']), harvest_object, 'Import')
                return False

            #"Spatial Administrative Area",
            #"Use Constraints",
            for e in package_dict.get('extras', ''):
                if e['key'] == 'Date Released':
                    package_dict['date_released'] = normalize_date(e['value'])
                elif e['key'] == 'Date Created':
                    package_dict['date_created'] = normalize_date(e['value'])
                elif e['key'] == 'Date Modified':
                    package_dict['date_modified'] = normalize_date(e['value'])
                elif e['key'] == 'date_released':
                    package_dict['date_released'] = normalize_date(e['value'])
                elif e['key'] == 'date_created':
                    package_dict['date_created'] = normalize_date(e['value'])
                elif e['key'] == 'date_modified':
                    package_dict['date_modified'] = normalize_date(e['value'])
                elif e['key'] == 'update_frequency':
                    package_dict['update_frequency'] = e['value']
                elif e['key'] == 'Date Range':
                    package_dict['temporal_coverage-other'] = e['value']
                elif e['key'] == 'Spatial Projection':
                    package_dict['spatial-reference-system'] = e['value']
                elif e['key'] == 'Geographical Bounding Box':
                    package_dict['geographic_coverage-other'] = e['value']
                elif e['key'] == 'Purpose of Collection':
                    package_dict['lineage'] = e['value']

            if 'date_released' not in package_dict:
                package_dict['date_released'] = normalize_date(package_dict['metadata_created'])
            if 'date_updated' not in package_dict:
                package_dict['date_updated'] = normalize_date(package_dict['metadata_modified'])

            if 'contact_point_name' in package_dict:
                package_dict['contact-name'] = package_dict['contact_point_name']
            elif 'contact_name' in package_dict:
                package_dict['contact-name'] = package_dict['contact_name']
            if 'contact_point_email' in package_dict:
                package_dict['contact-email'] = package_dict['contact_point_email']
            elif 'contact_email' in package_dict:
                package_dict['contact-email'] = package_dict['contact_email']
            if 'contact_point_phone' in package_dict:
                package_dict['contact-phone'] = package_dict['contact_point_phone']
            elif 'contact_phone' in package_dict:
                package_dict['contact-phone'] = package_dict['contact_phone']

            # for dublinked specifically
            if srcname == 'dublinked':
                if 'author' in package_dict:
                    package_dict['contact-name'] = package_dict['author']
                if 'author_email' in package_dict:
                    package_dict['contact-email'] = package_dict['author_email']
                if 'maintainer' in package_dict:
                    package_dict['contact-name'] = package_dict['maintainer']
                if 'maintainer_email' in package_dict:
                    package_dict['contact-email'] = package_dict['maintainer_email']

            if srcname == 'dublinked' and ('contact-email' not in package_dict or package_dict['contact-email'] == 'Not supplied'):
                package_dict['contact-email'] = 'info@dublinked.ie'

            if 'geographic_coverage' in package_dict:
                package_dict['geographic_coverage-other'] = package_dict['geographic_coverage']
                #This was an attempt to 'clear' geographic_coverage when I thought removal is impossible, but in any case it seems to get autofilled by the update action?
                #e_c = 0
                #for e in package_dict.get('extras', ''):
                #    if e['key'] == 'geographic_coverage':
                #        package_dict['extras'][e_c]['value'] = None
                #    e_c += 1
            elif 'spatial' in package_dict:
                package_dict['geographic_coverage-other'] = package_dict['spatial']

            # stupid ckan doesn't understand ISO dates
            if '-' in package_dict['date_released']:
                package_dict['date_released'] = arrow.get(package_dict['date_released'], ['YYYY-MM-DD HH:mm:ss', 'YYYY-MM-DD HH:mm', 'YYYY-MM-DD', 'YYYY-MM']).format('DD/MM/YYYY HH:mm')

            log.debug(package_dict)

            result = self._create_or_update_package(package_dict, harvest_object)

            log.debug(result)
            return result
        except ValidationError, e:
            self._save_object_error('Invalid package with GUID %s: %r' %
                                    (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
        except Exception, e:
            log.debug("Exception! %s of %s" % (e, package_dict['name']))
            self._save_object_error('Exception: %s' % e, harvest_object, 'Import')


def normalize_date (datestring):
    if not datestring:
        return None
    #iso format with degrees of accuracy
    formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d']
    lastErr = None
    for fmt in formats:
        try:
            #raises ValueError if it can't parse format string
            date = datetime.datetime.strptime(datestring, fmt)
            if date:
                return '%s/%s/%s %s:%s' % (date.day, date.month, date.year, date.hour, date.minute)
        except ValueError, err:
            continue

class ContentFetchError(Exception):
    pass

class ContentNotFoundError(ContentFetchError):
    pass

class RemoteResourceError(Exception):
    pass


class SearchError(Exception):
    pass

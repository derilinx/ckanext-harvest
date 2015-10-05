'''
Tests for ckanext/harvest/harvesters/base.py
'''
import re

from nose.tools import assert_equal

from ckanext.harvest.harvesters.base import munge_tags, HarvesterBase
try:
    from ckan.tests import helpers
    from ckan.tests import factories
except ImportError:
    from ckan.new_tests import helpers
    from ckan.new_tests import factories


class TestMungeTags:

    def test_basic(self):
        pkg = {'tags': [{'name': 'river quality'},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'river-quality'},
                                   {'name': 'geo'}])

    def test_blank(self):
        pkg = {'tags': [{'name': ''},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'geo'}])

    def test_replaced(self):
        pkg = {'tags': [{'name': '*'},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'geo'}])


def test_extras_from_dict():
    res = HarvesterBase.extras_from_dict({'theme': 'environment',
                                          'freq': 'daily'})
    assert_equal(res, [{'key': 'theme', 'value': 'environment'},
                       {'key': 'freq', 'value': 'daily'}])


def test_match_resources_with_existing_ones():
    res_dicts = [{'url': 'url1', 'name': 'name', 'description': 'desc'},
                 {'url': 'url3', 'name': 'name', 'description': 'desc'},
                 {'url': 'url2', 'name': 'name', 'description': 'desc'},
                 {'url': 'url4', 'name': 'name', 'description': 'desc'},
                 ]
    existing_resources = [
        {'id': '1', 'url': 'url1', 'name': 'name', 'description': 'desc'},
        {'id': 'BAD', 'url': 'BAD', 'name': 'name', 'description': 'desc'},
        {'id': '2', 'url': 'url2', 'name': 'BAD', 'description': 'desc'},
        {'id': '4', 'url': 'url4', 'name': 'BAD', 'description': 'desc'},
        ]
    HarvesterBase._match_resources_with_existing_ones(res_dicts,
                                                      existing_resources)
    assert_equal(res_dicts[0].get('id'), '1')
    assert_equal(res_dicts[1].get('id'), None)
    assert_equal(res_dicts[2].get('id'), '2')
    assert_equal(res_dicts[3].get('id'), '4')


_ensure_name_is_unique = HarvesterBase._ensure_name_is_unique


class TestGenNewName(object):
    @classmethod
    def setup_class(cls):
        helpers.reset_db()

    def test_basic(self):
        assert_equal(HarvesterBase._gen_new_name('Trees'), 'trees')

    def test_munge(self):
        assert_equal(
            HarvesterBase._gen_new_name('Trees and branches - survey.'),
            'trees-and-branches-survey')


class TestEnsureNameIsUnique(object):
    def setup(self):
        helpers.reset_db()

    def test_no_existing_datasets(self):
        factories.Dataset(name='unrelated')
        assert_equal(_ensure_name_is_unique('trees'), 'trees')

    def test_existing_dataset(self):
        factories.Dataset(name='trees')
        assert_equal(_ensure_name_is_unique('trees'), 'trees1')

    def test_two_existing_datasets(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees'), 'trees2')

    def test_no_existing_datasets_and_long_name(self):
        assert_equal(_ensure_name_is_unique('x'*101), 'x'*100)

    def test_existing_dataset_and_long_name(self):
        # because PACKAGE_NAME_MAX_LENGTH = 100
        factories.Dataset(name='x'*100)
        assert_equal(_ensure_name_is_unique('x'*101), 'x'*99 + '1')

    def test_update_dataset_with_new_name(self):
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('tree', existing_name='trees1'),
                     'tree')

    def test_update_dataset_but_with_same_name(self):
        # this can happen if you remove a trailing space from the title - the
        # harvester sees the title changed and thinks it should have a new
        # name, but clearly it can reuse its existing one
        factories.Dataset(name='trees')
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees'),
                     'trees')

    def test_update_dataset_to_available_shorter_name(self):
        # this can be handy when if reharvesting, you got duplicates and
        # managed to purge one set and through a minor title change you can now
        # lose the appended number. users don't like unnecessary numbers.
        factories.Dataset(name='trees1')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees1'),
                     'trees')

    def test_update_dataset_but_doesnt_change_to_other_number(self):
        # there's no point changing one number for another though
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        assert_equal(_ensure_name_is_unique('trees', existing_name='trees2'),
                     'trees2')

    def test_update_dataset_with_new_name_with_numbers(self):
        factories.Dataset(name='trees')
        factories.Dataset(name='trees2')
        factories.Dataset(name='frogs')
        assert_equal(_ensure_name_is_unique('frogs', existing_name='trees2'),
                     'frogs1')

    def test_existing_dataset_appending_hex(self):
        factories.Dataset(name='trees')
        name = _ensure_name_is_unique('trees', append_type='random-hex')
        # e.g. 'trees0b53f'
        assert re.match('trees[\da-f]{5}', name)

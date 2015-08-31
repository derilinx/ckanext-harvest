'''
Tests for ckanext/harvest/harvesters/base.py
'''
from nose.tools import assert_equal
from ckanext.harvest.harvesters.base import munge_tags, HarvesterBase


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

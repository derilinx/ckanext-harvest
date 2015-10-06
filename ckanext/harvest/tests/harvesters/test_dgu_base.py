from nose.tools import assert_equal

from ckanext.harvest.harvesters.dgu_base import DguHarvesterBase


def test_extras_from_dict():
    res = DguHarvesterBase.extras_from_dict({'theme': 'environment',
                                             'freq': 'daily'})
    assert_equal(res, [{'key': 'theme', 'value': 'environment'},
                       {'key': 'freq', 'value': 'daily'}])

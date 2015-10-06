from nose.tools import assert_equal

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

try:
    from ckan.tests import helpers
    from ckan.tests import factories
except ImportError:
    from ckan.new_tests import helpers
    from ckan.new_tests import factories

get_name = CKANHarvester.get_name


class TestGetName(object):
    def setup(self):
        helpers.reset_db()

    def test_new_dataset_no_clash(self):
        assert_equal(get_name(None, 'Trees', None), 'trees')

    def test_new_dataset_clash(self):
        factories.Dataset(name='trees')
        assert_equal(get_name('trees', 'Trees', None), 'trees1')

    def test_new_dataset_clash_and_title_differs_from_name(self):
        # mild preference to be similar to original name, rather than base it
        # on the title
        factories.Dataset(name='trees')
        assert_equal(get_name('trees', 'Birds', None), 'trees1')

    def test_update_dataset_no_clash(self):
        factories.Dataset(name='trees')
        assert_equal(get_name('trees', 'Trees', 'trees'), 'trees')

    def test_update_dataset_changed_title(self):
        # keeps remote name
        factories.Dataset(name='trees')
        assert_equal(get_name('trees', 'Birds', 'trees'), 'trees')

    def test_update_dataset_lose_number(self):
        # mildly helpful
        factories.Dataset(name='trees1')
        assert_equal(get_name('trees', 'Trees', 'trees1'), 'trees')

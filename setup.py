from setuptools import setup, find_packages

version = '1.5.3'

setup(
<<<<<<< HEAD
    name='ckanext-harvest',
    version=version,
    description="Harvesting interface plugin for CKAN",
    long_description="""\
    """,
    classifiers=[],
    keywords='',
    author='CKAN',
    author_email='ckan@okfn.org',
    url='https://github.com/ckan/ckanext-harvest',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "ckantoolkit>=0.0.7",
        "pika>=1.1.0,<1.3.0",
        "redis<3.0",
        "requests>=2.11.1"
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    test_suite='nose.collector',
    entry_points="""
        [ckan.plugins]
            # Add plugins here, eg
            harvest=ckanext.harvest.plugin:Harvest
            ckan_harvester=ckanext.harvest.harvesters:CKANHarvester

            # Test plugins

            test_harvester=ckanext.harvest.tests.test_queue:MockHarvester
            test_harvester2=ckanext.harvest.tests.test_queue2:MockHarvester
            test_action_harvester=ckanext.harvest.tests.test_action:MockHarvesterForActionTests

        [paste.paster_command]
            harvester = ckanext.harvest.commands.harvester:Harvester
        [babel.extractors]
            ckan = ckan.lib.extract:extract_ckan
    """,
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    }
)
=======
	name='ckanext-harvest',
	version=version,
	description="Harvesting interface plugin for CKAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN',
	author_email='ckan@okfn.org',
	url='http://ckan.org/wiki/Extensions',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
	    'pika==0.9.8',
	    'redis<3.0'
	],
	tests_require=[
		'nose',
		'mock',
	],
	test_suite = 'nose.collector',
	entry_points=\
	"""
    [ckan.plugins]
	# Add plugins here, eg
	harvest=ckanext.harvest.plugin:Harvest
	ckan_harvester=ckanext.harvest.harvesters:CKANHarvester
    [ckan.test_plugins]
	test_harvester=ckanext.harvest.tests.test_queue:MockHarvester
	test_harvester2=ckanext.harvest.tests.test_queue2:MockHarvester
	test_action_harvester=ckanext.harvest.tests.test_action:MockHarvesterForActionTests
	[paste.paster_command]
	harvester = ckanext.harvest.commands.harvester:Harvester
    [babel.extractors]
    ckan = ckan.lib.extract:extract_ckan
	""",
        message_extractors={
            'ckanext': [
                ('**.py', 'python', None),
                ('**.js', 'javascript', None),
                ('**/templates/**.html', 'ckan', None),
            ],
        }
)
>>>>>>> 5188150 (Pin broadly compatible redis releases)

#!/bin/bash
set -e

if [ $CKANVERSION == '2.2-dgu' ]
then
    nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini ckanext/harvest
else
    # dont run test_queue as it relies on rabbitmq
    nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini -I test_queue ckanext/harvest
fi

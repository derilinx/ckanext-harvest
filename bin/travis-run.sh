#!/bin/bash
set -e
set -x  # echo commands as they are run

if [ $CKANVERSION == '2.2-dgu' ]
then
    nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini ckanext/harvest
else
    # dont run test_queue as it relies on rabbitmq
    nosetests --ckan --nologcapture --with-pylons=subdir/test-core.ini -I test_queue ckanext/harvest
fi

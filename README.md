ckanext-ogdchcommands
=====================

CKAN extension for DCAT-AP Switzerland
providing commands to run in the background

## Requirements

- CKAN 2.8+
- ckanext-switzerland
- ckanext-harvest
- ckanext-datastore


## Command to cleanup the datastore database.
[Datastore currently does not delete tables](https://github.com/ckan/ckan/issues/3422) when the corresponding resource is deleted.
This command finds these orphaned tables and deletes its rows to free the space in the database.
It is meant to be run regularly by a cronjob.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_datastore -c /var/www/ckan/development.ini
```

## Command to cleanup the harvest jobs.
This commands deletes the harvest jobs and objects per source and overall leaving only the latest n,
where n and the source are optional arguments. The command is supposed to be used in a cron job to 
provide for a regular cleanup of harvest jobs, so that the database is not overloaded with unneeded data
of past job runs. It has a dryrun option so that it can be tested what will get be deleted in the 
database before the actual database changes are performed.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_harvestjobs [{source_id}] [--keep={n}}] [--dryrun] -c /var/www/ckan/development.ini
```

## Command for Shacl Validation
The Swiss Dcat Harvester offers a validation where the data is tested against a shacl shape graph.
- the validation currently uses https://jena.apache.org/documentation/shacl/index.html
- these parameters need to be in the ckan configuration file:
```
ckanext.ogdchcommands.shacl_command_path = /opt/apache-jena-3.13.1/bin/shacl
ckanext.ogdchcommands.shacl_results_path = /home/liipadmin/shaclresults
ckanext.ogdchcommands.shacl_shapes_path = /home/liipadmin/shaclshapes
``` 
The validation is implemented as a command:

```bash
paster --plugin=ckanext-ogdchcommands ogdch shacl_validate {source_id} [--shapefile={shapefilename}}] -c /var/www/ckan/development.ini
```
The command `/opt/apache-jena-3.13.1/bin/shacl` from https://jena.apache.org/documentation/shacl/index.html is expected to be implemented at `ckanext.ogdchcommands.shacl_command_path` 

All shacl shapes are expected to be in `ckanext.ogdchcommands.shacl_shapes_path`
- the default shapefile is `ech-0200.shacl.ttl` from https://github.com/factsmission/dcat-ap-ch-shacl


There are 3 output files written to `ckanext.ogdchcommands.shacl_results_path`: 
- they are gathered under a directory `<harvest_source_id>` 
- `data.ttl` is the harvest source serialized as turtle
- `result.ttl` is the raw output of the apache-jena shacl validation command
- `result.csv` is a csv file with all shacl validation errors

The csv file has the following headers:
- `sh_focusnode` : SHACL.focusNode
- `sh_path` : SHACL.resultPath
- `sh_severity` : SHACL.resultSeverity
- `sh_constraint` : SHACL.sourceConstraintComponent
- `sh_message` : SHACL.resultMessage
- `sh_detail` : SHACL.resultDetail
- `sh_shape` : SHACL.sourceShape
- `sh_value`: SHACL.value
- `harvest_source_id`: harvest_source_id
- `parseerror`: parse errors that occurred

## Installation

To install ckanext-ogdchcommands:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-ogdchcommands Python package into your virtual environment:

     pip install ckanext-ogdchcommands

3. Add ``ogdch_cmd`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload

## Config Settings

This extension uses the following config options (.ini file)

    # number of harvest jobs to keep per harvest source when cleaning up harvest objects   
    ckanext.ogdchcommands.number_harvest_jobs_per_source = 2


## Development Installation

To install ckanext-ogdchcommands for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/ogdch/ogdchcommands.git
    cd ckanext-ogdchcommands
    python setup.py develop
    pip install -r dev-requirements.txt
    pip install -r requirements.txt


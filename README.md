ckanext-ogdchcommands
=====================

CKAN extension for DCAT-AP Switzerland. This extension provides two CKAN plugins:

- `ogdch_cmd`: providing commands to run in the background
- `ogdch_admin`: admin tools mainly for handeling of solr

## Requirements

- CKAN 2.8+
- ckanext-switzerland
- ckanext-harvest
- ckanext-datastore

## `ogdch_cmd` Paster Commands


### Command to cleanup the datastore database.
[Datastore currently does not delete tables](https://github.com/ckan/ckan/issues/3422) when the corresponding resource is deleted.
This command finds these orphaned tables and deletes its rows to free the space in the database.
It is meant to be run regularly by a cronjob.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_datastore -c /var/www/ckan/development.ini
```

## Command to cleanup the resources.
When datasets are harvested, we try to reuse the existing resources, but not all of them are 
reused. Some old resources remain with the state 'deleted'. These orphaned resources can be
deleted with this command. It is meant to be run regularly by a cronjob. 
It also comes with a dryrun option.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_resources -c /var/www/ckan/development.ini
```

## Command to cleanup the package extra table.
When a key is no longer needed in the package_extra table, since it is no longer part of the dataset,
then after the data have been migrated that old key can be removed from the package_extra table 
and from the dependent table package_extra_revision.
The command comes with a dryrun option.

```bash
paster --plugin=ckanext-ogdchcommands ogdch cleanup_extras publishers --dryrun -c /var/www/ckan/development.ini
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

### Command to publish private datasets that have a scheduled-date.
This command will look for private datasets that have the `scheduled`-field set and will publish it if it is due.
```bash
paster --plugin=ckanext-ogdchcommands ogdch publish_scheduled_datasets [--dryrun] -c /var/www/ckan/development.ini
```

## Command to clear stale harvest sources.
This commands clears all datasets, jobs and objects related to a harvest source 
that was not active for a given amount of days (default 30 days).
The command is supposed to be used in a cron job and to check all harvest sources.

```bash
paster --plugin=ckanext-ogdchcommands ogdch clear_stale_harvestsources [--keep_harvestsource_days={n}}] -c /var/www/ckan/development.ini
```

## `ogdch_admin` Admin Tools

The following Api Calls can be used if this plugin is installed:

- `https://ckan.ogdch-test.clients.liip.ch/api/3/action/ogdch_check_indexing`

checks whether there are any unindexed packages in CKAN

- `https://ckan.ogdch-test.clients.liip.ch/api/3/action/ogdch_reindex`

reindexes Solr. You can use it with these arguments: `package_id=<name of the dataset>` and `only_missing=true`
In the later case only datasets missing in the index will get reindexed

- `https://ckan.ogdch-test.clients.liip.ch/api/3/action/ogdch_check_field?field=<name of the field>>`

This checks the database and looks for the given fields in there: the field values will be reported back together
with the dataset name.

## Installation

To install ckanext-ogdchcommands:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-ogdchcommands Python package into your virtual environment:

     pip install ckanext-ogdchcommands

3. Add ``ogdch_cmd`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``) if you want to use the paster commands
4. Add ``ogdch_admin`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``) if you want to use the solr admin tools
   

5. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload

## Config Settings

This `ogdch_cmd` uses the following config options (.ini file)

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

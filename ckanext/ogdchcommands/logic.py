import itertools

from ckan.logic import NotFound, ValidationError
import ckan.plugins.toolkit as tk
from ckan import model
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject

import datetime
import logging
log = logging.getLogger(__name__)

FORMAT_TURTLE = 'ttl'
DATA_IDENTIFIER = 'data'
RESULT_IDENTIFIER = 'result'


def ogdch_cleanup_harvestjobs(context, data_dict):
    """
    cleans up the database for harvest objects and related tables for all
    harvesting jobs except the latest
    'ckanext.switzerland.number_harvest_jobs_per_source' is the corresponding
    configuration parameter on how many jobs to keep per source
    The command can be called with or without a source. In the later case all
    sources are cleaned.
    """

    # check access rights
    tk.check_access('harvest_sources_clear', context, data_dict)
    model = context['model']

    # get sources from data_dict
    if 'harvest_source_id' in data_dict:
        harvest_source_id = data_dict['harvest_source_id']
        source = HarvestSource.get(harvest_source_id)
        if not source:
            log.error('Harvest source {} does not exist'.format(
                harvest_source_id))
            raise NotFound('Harvest source {} does not exist'.format(
                harvest_source_id))
        sources_to_cleanup = [source]
    else:
        sources_to_cleanup = model.Session.query(HarvestSource).all()

    # get number of jobs to keep form data_dict
    if 'number_of_jobs_to_keep' in data_dict:
        number_of_jobs_to_keep = data_dict['number_of_jobs_to_keep']
    else:
        log.error(
            'Configuration missing for number of harvest jobs to keep')
        raise ValidationError(
            'Configuration missing for number of harvest jobs to keep')

    dryrun = data_dict.get("dryrun", False)

    log.info('Harvest job cleanup called for sources: {},'
             'configuration: {}'.format(
                 ', '.join([s.id for s in sources_to_cleanup]),
                 data_dict))

    # store cleanup result
    cleanup_result = {}
    for source in sources_to_cleanup:

        # get jobs ordered by their creations date
        delete_jobs = model.Session.query(HarvestJob) \
            .filter(HarvestJob.source_id == source.id) \
            .filter(HarvestJob.status == 'Finished') \
            .order_by(HarvestJob.created.desc()).all()[number_of_jobs_to_keep:]

        # decide which jobs to keep or delete on their order
        delete_jobs_ids = [job.id for job in delete_jobs]

        if not delete_jobs:
            log.debug(
                'Cleanup harvest jobs for source {}: nothing to do'
                .format(source.id))
        else:
            # log all job for a source with the decision to delete or keep them
            log.debug('Cleanup harvest jobs for source {}: delete jobs: {}'
                      .format(source.id, delete_jobs_ids))

            # get harvest objects for harvest jobs
            delete_objects_ids = \
                model.Session.query(HarvestObject.id) \
                .filter(HarvestObject.harvest_job_id.in_(
                    delete_jobs_ids)).all()
            delete_objects_ids = list(itertools.chain(
                *delete_objects_ids))

            # log all objects to delete
            log.debug(
                'Cleanup harvest objects for source {}: delete {} objects'
                .format(source.id, len(delete_objects_ids)))

            # perform delete
            sql = '''begin;
            delete from harvest_object_error
            where harvest_object_id in ('{delete_objects_values}');
            delete from harvest_object_extra
            where harvest_object_id in ('{delete_objects_values}');
            delete from harvest_object
            where id in ('{delete_objects_values}');
            delete from harvest_gather_error
            where harvest_job_id in ('{delete_jobs_values}');
            delete from harvest_job
            where id in ('{delete_jobs_values}');
            commit;
            '''.format(delete_objects_values="','".join(delete_objects_ids),
                       delete_jobs_values="','".join(delete_jobs_ids))

            # only execute the sql if it is not a dry run
            if not dryrun:
                model.Session.execute(sql)

                # reindex after deletions
                tk.get_action('harvest_source_reindex')(
                    context, {'id': source.id})

            # fill result
            cleanup_result[source.id] = {
                'deleted_jobs': delete_jobs,
                'deleted_nr_objects': len(delete_objects_ids)}

            log.info(
                'cleaned resource and shacl result directories {}'
                .format(source.id))

    # return result of action
    return {'sources': sources_to_cleanup,
            'cleanup': cleanup_result}


def ogdch_cleanup_resources(context, data_dict):
    """
    cleans up the database from resources that have been deleted
    """
    dryrun = data_dict.get('dryrun')
    tk.check_access('resource_delete', context, data_dict)
    delete_resources = model.Session.query(model.Resource) \
        .filter(model.Resource.state == 'deleted') \
        .all()
    delete_resources_ids = [resource.id for resource in delete_resources]
    count = len(delete_resources_ids)

    sql = '''begin;
    delete from resource_view
    where resource_id in ('{delete_id_values}');
    delete from resource_revision
    where continuity_id in ('{delete_id_values}');
    delete from resource
    where id in ('{delete_id_values}');
    commit;
    '''.format(delete_id_values="','".join(delete_resources_ids))

    if not dryrun:
        model.Session.execute(sql)
        log.debug("{} resources have been deleted together with their "
                  "dependencies: resource_revision and resource_view"
                  .format(count))
    return {
        "count_deleted": count,
        "dryrun": dryrun,
    }


def cleanup_package_extra(context, data_dict):
    """
    cleans up package_extra table for a given key
    """
    dryrun = data_dict.get('dryrun')
    key = data_dict.get('key')
    tk.check_access('package_delete', context, data_dict)
    delete_package_extras = model.Session.query(model.PackageExtra) \
        .filter(model.PackageExtra.key == key) \
        .all()
    delete_package_extra_ids = [extra.id for extra in delete_package_extras]
    count = len(delete_package_extra_ids)

    sql = '''begin;
    delete from package_extra_revision
    where continuity_id in ('{delete_id_values}');
    delete from package_extra
    where id in  ('{delete_id_values}');
    commit;
    '''.format(delete_id_values="','".join(delete_package_extra_ids))

    if not dryrun:
        model.Session.execute(sql)
        log.debug("{} package_extras have been deleted"
                  .format(count))
    return {
        "count_deleted": count,
        "dryrun": dryrun,
    }

def ogdch_cleanup_harvestsource(context, data_dict):
    """
    Cleaning up jobs for all harvest sources
    """

    # get the last day to keep harvested datasets
    tf_to_keep_harvested_dsets = data_dict.get('tf_to_keep_harvested_dsets')
    last_day_to_keep_harvested_ds = datetime.datetime.now() - datetime.timedelta(tf_to_keep_harvested_dsets)

    # gets all active harvest sources
    harvest_sources = tk.get_action('harvest_source_list')(context, data_dict)
    count_cleared_harvestsource = 0

    for source in harvest_sources:
        source_dict = tk.get_action('harvest_source_show')(context, {
            'id': source['id']
        })
        # check if there are any harvest jobs
        if not source_dict['status']['last_job']:
            log.info('No jobs yet for this harvest source id={}'.format(source['id']))
        else:
            jast_job_id = source_dict['status']['last_job']['id']
            last_job_creation_time = source_dict['status']['last_job']['created']
            last_job_creation_time_obj = datetime.datetime.strptime(last_job_creation_time,
                                                                    "%Y-%m-%d %H:%M:%S.%f")

            if (last_job_creation_time_obj < last_day_to_keep_harvested_ds):
                log.info('Harvest latest job id={} with creation_time={} is older than {} days'
                      .format(jast_job_id, last_job_creation_time,
                              tf_to_keep_harvested_dsets))
                log.info('Clears all datasets, jobs and objects related to a harvest source id={}'
                      .format(jast_job_id))
                count_cleared_harvestsource += count_cleared_harvestsource
                tk.get_action("harvest_source_clear")(context, {"id": source['id']})
            else:
                log.info('Harvest job id={} with creation_time={} is not older than {} days'
                      .format(jast_job_id, last_job_creation_time,
                              tf_to_keep_harvested_dsets))

    return {
            "count_cleared_harvestsource": count_cleared_harvestsource,
    }

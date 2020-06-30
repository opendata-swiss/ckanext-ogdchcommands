import itertools
import json
import csv
import subprocess
import rdflib

from ckan.logic import NotFound, ValidationError
from ckan.exceptions import CkanConfigurationException
import ckan.plugins.toolkit as tk
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.dcat.processors import RDFParserException
from ckanext.ogdchcommands.shaclprocessor import (
    ShaclParser, SHACLParserException)

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

            log.error(
                'cleaned resource and shacl result directories {}'
                .format(source.id))

    # return result of action
    return {'sources': sources_to_cleanup,
            'cleanup': cleanup_result}


def ogdch_shacl_validate(context, data_dict):  # noqa
    """
    validates a harvest source against a shacl shape
    """

    # get sources from data_dict
    if 'harvest_source_id' in data_dict:
        harvest_source_id = data_dict['harvest_source_id']
        harvest_source = HarvestSource.get(harvest_source_id)
        if not harvest_source:
            raise NotFound('Harvest source {} does not exist'.format(
                harvest_source_id))
    else:
        raise NotFound('Configuration missing for harvest source')

    datapath = data_dict['datapath']
    resultpath = data_dict['resultpath']
    shapefilepath = data_dict['shapefilepath']
    csvpath = data_dict['csvpath']
    shaclcommand = data_dict['shaclcommand']

    log.info('shacl_validate called for source: {},'
             'configuration: {}'
             .format(harvest_source_id, data_dict))

    # get rdf parse config for harvest source
    rdf_format = json.loads(harvest_source.config)\
        .get("rdf_format", "xml")

    # parse harvest_source
    data_rdfgraph = rdflib.Graph()

    # parse data from harvest source url
    try:
        data_rdfgraph.parse(harvest_source.url, format=rdf_format)
    except RDFParserException, e:
        raise RDFParserException(
            'Error parsing the RDF file during shacl validation: {0}'
            .format(e))

    log.debug("parsed source url {} with format {}"
              .format(harvest_source.url, rdf_format))

    # write parsed data to file
    try:
        with open(datapath, 'w') as datawriter:
            datawriter.write(data_rdfgraph.serialize(format='turtle'))
    except CkanConfigurationException as e:
        raise CkanConfigurationException(
            'Configuration during shacl validation: {0}'
            .format(e))

    log.debug("datagraph was serialized to turtle: {}"
              .format(datapath))

    # execute the shacl command
    try:
        with open(resultpath, 'w') as resultwriter:
            subprocess.call(
                [shaclcommand,
                 "validate",
                 "--shapes", shapefilepath,
                 "--data", datapath],
                stdout=resultwriter)
    except CkanConfigurationException as e:
        raise CkanConfigurationException(
            'Configuration during shacl validation: {0}'
            .format(e))

    log.debug("shacl command was executed: {}"
              .format(resultpath))

    shaclparser = ShaclParser(resultpath, harvest_source_id)
    try:
        shaclparser.parse()
    except SHACLParserException as e:
        raise CkanConfigurationException(
            'Exception parsing result: {0}. Please try again.'
            .format(e))

    log.debug("shacl parser is initialized: {}"
              .format(resultpath, harvest_source_id))

    # write shacl errors to csv file
    with open(csvpath, 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=shaclparser.resultdictkeys,
            delimiter='|', restval='')
        writer.writeheader()
        for resultdict in shaclparser.shaclresults():
            try:
                writer.writerow(resultdict)
            except UnicodeEncodeError as e:
                resultdict = {
                    shaclparser.resultdictkey_harvestsourceid:
                        harvest_source_id,
                    shaclparser.resultdictkey_parseerror: e
                }
                writer.writerow(resultdict)

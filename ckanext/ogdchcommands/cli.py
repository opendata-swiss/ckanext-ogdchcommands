import itertools
import sys
import traceback
from datetime import datetime

import ckan.logic as logic
import ckan.model as model
import click

msg_resource_cleanup_dryrun = """Resources cleanup:
==================
There are {0} resources in status 'deleted'.
There are {1} filestore-entries associated with those resources that can be deleted.
{2}
If you want to delete them, run this command
again without the option --dryrun!"""

msg_resource_cleanup = """Resources cleanup:
==================
{} resources in status 'deleted' have been deleted."""

msg_package_extra_cleanup_dryrun = """\npackage extra cleanup for key '{1}':\n\n
There are {0} package extras with key '{1}'.
If you want to delete them, run this command
again without the option --dryrun!\n"""

msg_package_extra_cleanup = """\npackage extra cleanup for key '{1}':\n\n
{0} package extras with key '{1}' have been deleted.\n"""

msg_filestore_cleanup_dryrun = """Filestore cleanup:
==================
There are {0} filestore-entries that are not associated to any resource in the database which can probably be deleted.
{1}
Following errors occured, please check those cases manually:
{2}
If you want to delete them, run this command
again without the option --dryrun!"""

msg_filestore_cleanup = """Filestore cleanup:
==================
{0} filestore-entries that were not associated with any resource in the database have been deleted.
{1}
Following errors occured, please check those cases manually:
{2}
"""


def get_commands():
    return [ogdch]


@click.group()
def ogdch():
    pass


@ogdch.command()
@click.option(
    "--dryrun",
    is_flag=True,
    required=False,
    help="See what would happen on running this command without making any real changes",
)
def publish_scheduled_datasets(dryrun):
    """Publish scheduled datasets.

    Checks for private datasets that have a scheduled date that is either today
    or in the past, and sets them to public
    """

    user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"model": model, "session": model.Session, "user": user["name"]}
    try:
        logic.check_access("package_patch", context)
    except logic.NotAuthorized:
        print("User is not authorized to perform this action.")
        sys.exit(1)

    query = logic.get_action("package_search")(
        context,
        {
            "q": "*:*",
            "fq": "+capacity:private +scheduled:[* TO *]",
            "include_private": True,
        },
    )
    private_datasets = query["results"]
    log_output = """Private datasets that are due to be published: \n\n"""
    for dataset in private_datasets:
        data_dict = _is_dataset_due_to_be_published(context, dataset)
        if data_dict:
            log_output += 'Private dataset: "%s" (%s) ... ' % (
                data_dict.get("name"),
                data_dict.get("scheduled"),
            )
            if not dryrun:
                data_dict["private"] = False
                logic.get_action("package_patch")(context, data_dict)
                log_output += "has been published.\n"
            else:
                log_output += "is due to be published.\n"

    print(log_output)

    if dryrun:
        print(
            "\nThis has been a dry run: "
            "if you want to perfom these changes"
            " run this again without the option --dryrun!"
        )
    else:
        print(
            "\nPrivate datasets that are due have been published. "
            "See output above about what has been done."
        )


@ogdch.command()
def cleanup_datastore():
    """Clean up datastore."""
    user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"model": model, "session": model.Session, "user": user["name"]}
    try:
        logic.check_access("datastore_delete", context)
        logic.check_access("resource_show", context)
    except logic.NotAuthorized:
        print("User is not authorized to perform this action.")
        sys.exit(1)

    # query datastore to get all resources from the _table_metadata
    resource_id_list = []
    try:
        for offset in itertools.count(start=0, step=100):
            print("Load metadata records from datastore (offset: %s)" % offset)
            record_list, has_next_page = _get_datastore_table_page(context, offset)
            resource_id_list.extend(record_list)
            if not has_next_page:
                break
    except Exception as e:
        print(
            "Error while gathering resources: %s / %s"
            % (str(e), traceback.format_exc())
        )

    # delete the rows of the orphaned datastore tables
    delete_count = 0
    for resource_id in resource_id_list:
        logic.check_access("datastore_delete", context)
        logic.get_action("datastore_delete")(
            context, {"resource_id": resource_id, "force": True}
        )
        print("Table '%s' deleted (not dropped)" % resource_id)
        delete_count += 1

    print("Deleted content of %s tables" % delete_count)


@ogdch.command()
@click.option(
    "--dryrun",
    is_flag=True,
    required=False,
    help="See what would happen on running this command without making any real changes",
)
def cleanup_filestore(dryrun):
    """Cleanup filestore

    Delete filestore files that are no longer associated with a resource.
    """
    user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"model": model, "session": model.Session, "user": user["name"]}
    result = logic.get_action("ogdch_cleanup_filestore")(
        context,
        {
            "dryrun": dryrun,
        },
    )
    if dryrun:
        print(
            msg_filestore_cleanup_dryrun.format(
                result.get("file_count"), result.get("filepaths"), result.get("errors")
            )
        )
    else:
        print(
            msg_filestore_cleanup.format(
                result.get("file_count"), result.get("filepaths"), result.get("errors")
            )
        )


@ogdch.command()
@click.option(
    "--dryrun",
    is_flag=True,
    required=False,
    help="See what would happen on running this command without making any real changes",
)
def cleanup_resources(dryrun):
    """Clean up resources in the db.

    Remove resources that have the state 'deleted' from the database.
    Also cleans their dependencies in resource_view and resource_revision.
    """
    user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"model": model, "session": model.Session, "user": user["name"]}
    try:
        logic.check_access("resource_delete", context)
    except logic.NotAuthorized:
        print("User is not authorized to perform this action.")
        sys.exit(1)
    result = logic.get_action("ogdch_cleanup_resources")(
        context,
        {
            "dryrun": dryrun,
        },
    )
    if dryrun:
        print(
            msg_resource_cleanup_dryrun.format(
                result.get("count_deleted"),
                result.get("count_filestores"),
                result.get("filepaths"),
            )
        )
    else:
        print(
            msg_resource_cleanup.format(
                result.get(
                    "count_deleted",
                    result.get("count_filestores"),
                    result.get("filepaths"),
                )
            )
        )


@ogdch.command()
@click.argument("key", metavar="KEY", required=True)
@click.option(
    "--dryrun",
    is_flag=True,
    required=False,
    help="See what would happen on running this command without making any real changes",
)
def cleanup_extras(key, dryrun):
    """Cleanup package extras.

    Command for cleaning up the database after a key (field) has
    been removed in the dataset schema: all records for this key
    can then be deleted by running this command for the key.
    """
    if not key:
        print("Please provide a key for which extras should be cleaned.")
        sys.exit(1)
    user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
    context = {"model": model, "session": model.Session, "user": user["name"]}
    try:
        logic.check_access("package_delete", context)
    except logic.NotAuthorized:
        print("User is not authorized to perform this action.")
        sys.exit(1)
    result = logic.get_action("cleanup_package_extra")(
        context, {"dryrun": dryrun, "key": key}
    )
    if dryrun:
        print(msg_package_extra_cleanup_dryrun.format(result.get("count_deleted"), key))
    else:
        print(msg_package_extra_cleanup.format(result.get("count_deleted"), key))


@ogdch.command()
@click.argument("source_id", metavar="HARVEST_SOURCE_ID", required=False)
@click.argument("nr_of_jobs_to_keep", metavar="NR_OF_JOBS_TO_KEEP", required=False)
@click.option(
    "--dryrun",
    is_flag=True,
    required=False,
    help="See what would happen on running this command without making any real changes",
)
def cleanup_harvestjobs(nr_of_jobs_to_keep=10, dryrun=False, source_id=None):
    """Clean up harvester jobs and objects.

    Deletes all the harvest jobs and objects except the latest n.
    The default number of jobs to keep is 10.
    """
    data_dict = {
        "number_of_jobs_to_keep": nr_of_jobs_to_keep,
        "dryrun": dryrun,
    }
    if source_id:
        data_dict["harvest_source_id"] = source_id
        print("cleaning up jobs for harvest source {}".format(source_id))
    else:
        print("cleaning up jobs for all harvest sources")

    # set context
    context = {"model": model, "session": model.Session, "ignore_auth": True}
    admin_user = logic.get_action("get_site_user")(context, {})
    context["user"] = admin_user["name"]

    # test authorization
    try:
        logic.check_access("harvest_sources_clear", context, data_dict)
    except logic.NotAuthorized:
        print("User is not authorized to perform this action.")
        sys.exit(1)

    # perform the harvest job cleanup
    result = logic.get_action("ogdch_cleanup_harvestjobs")(context, data_dict)

    # print the result of the harvest job cleanup
    _print_clean_harvestjobs_result(result, data_dict)


@ogdch.command()
@click.argument(
    "timeframe_to_keep_harvested_datasets",
    metavar="DAYS_TO_KEEP_DATASETS",
    required=False,
)
def clear_stale_harvestsources(timeframe_to_keep_harvested_datasets):
    """Clean up harvest sources.

    Harvesters whose last jobs were finished more than n days ago will be
    cleared: all datasets, jobs and objects will be deleted, but the source
    itself will be kept.

    The default time to keep harvested datasets is 30 days.
    """
    data_dict = {
        "timeframe_to_keep_harvested_datasets": timeframe_to_keep_harvested_datasets,
    }

    # set context
    context = {"model": model, "session": model.Session, "ignore_auth": True}
    admin_user = logic.get_action("get_site_user")(context, {})
    context["user"] = admin_user["name"]

    # test authorization
    try:
        logic.check_access("harvest_sources_clear", context, data_dict)
        print("User is authorized to perform this action")
    except logic.NotAuthorized:
        print("User is not authorized to perform this action")
        sys.exit(1)

    # cleanup harvest source
    nr_cleanup_harvesters = logic.get_action("ogdch_cleanup_harvestsource")(
        context,
        {"timeframe_to_keep_harvested_datasets": timeframe_to_keep_harvested_datasets},
    )
    print(
        "{} harvest sources were cleared".format(
            nr_cleanup_harvesters["count_cleared_harvestsource"]
        )
    )


def _is_dataset_due_to_be_published(context, dataset):
    issued_datetime = datetime.strptime(dataset.get("scheduled"), "%d.%m.%Y")
    if issued_datetime.date() <= datetime.today().date():
        return logic.get_action("package_show")(context, {"id": dataset.get("id")})
    else:
        return None


def _get_datastore_table_page(context, offset=0):
    # query datastore to get all resources from the _table_metadata
    result = logic.get_action("datastore_search")(
        context, {"resource_id": "_table_metadata", "offset": offset}
    )

    resource_id_list = []
    for record in result["records"]:
        try:
            # ignore 'alias' records
            if record["alias_of"]:
                continue

            logic.check_access("resource_show", context)
            logic.get_action("resource_show")(context, {"id": record["name"]})
            print("Resource '%s' found" % record["name"])
        except logic.NotFound:
            resource_id_list.append(record["name"])
            print("Resource '%s' *not* found" % record["name"])
        except logic.NotAuthorized:
            print("User is not authorized to perform this action.")
        except (KeyError, AttributeError) as e:
            print("Error while handling record %s: %s" % (record, str(e)))
            continue

    # are there more records?
    has_next_page = len(result["records"]) > 0

    return resource_id_list, has_next_page


def _print_clean_harvestjobs_result(result, data_dict):
    print(
        "\nCleaning up jobs for harvest sources:\n{}\nConfiguration:".format(37 * "-")
    )
    _print_configuration(data_dict)
    print("\nResults per source:\n{}".format(19 * "-"))
    for source in result["sources"]:
        if source.id in result["cleanup"].keys():
            _print_harvest_source(source)
            _print_cleanup_result_per_source(result["cleanup"][source.id])
        else:
            _print_harvest_source(source)
            print("Nothing needs to be done for this source")

    if data_dict["dryrun"]:
        print(
            "\nThis has been a dry run: "
            "if you want to perfom these changes"
            " run this again without the option --dryrun!"
        )
    else:
        print(
            "\nThe database has been cleaned from harvester "
            "jobs and harvester objects."
            " See above about what has been done."
        )


def _print_harvest_source(source):
    print("\n           Source id: {0}".format(source.id))
    print("                 url: {0}".format(source.url))
    print("                type: {0}".format(source.type))


def _print_cleanup_result_per_source(cleanup_result):
    print("   nr jobs to delete: {0}".format(len(cleanup_result["deleted_jobs"])))
    print("nr objects to delete: {0}".format(cleanup_result["deleted_nr_objects"]))
    print("      jobs to delete:")
    _print_harvest_jobs(cleanup_result["deleted_jobs"])


def _print_configuration(data_dict):
    for k, v in data_dict.items():
        print("- {}: {}".format(k, v))


def _print_harvest_jobs(jobs):
    header_list = ["id", "created", "status"]
    row_format = "{:<20}|{:<40}|{:<20}|{:<20}"
    print(row_format.format("", *header_list))
    print("{:<20}+{:<40}+{:<20}+{:<20}".format("", "-" * 40, "-" * 20, "-" * 20))
    for job in jobs:
        print(
            row_format.format(
                "", job.id, job.created.strftime("%Y-%m-%d %H:%M:%S"), job.status
            )
        )

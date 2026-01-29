# coding=UTF-8

import logging
import os

import ckan.plugins as plugins

import ckanext.ogdchcommands.admin_logic as admin
import ckanext.ogdchcommands.logic as logic
from ckanext.ogdchcommands.cli import get_commands

log = logging.getLogger(__name__)

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class OgdchCommandsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IClick)

    # ------------------------------------------------------------
    # IActions
    # ------------------------------------------------------------
    # Allow adding of actions to the logic layer

    def get_actions(self):
        """
        Actions that are used by the commands.
        """
        return {
            "ogdch_cleanup_harvestjobs": logic.ogdch_cleanup_harvestjobs,
            "ogdch_cleanup_resources": logic.ogdch_cleanup_resources,
            "ogdch_cleanup_filestore": logic.ogdch_cleanup_filestore,
            "cleanup_package_extra": logic.cleanup_package_extra,
            "ogdch_cleanup_harvestsource": logic.ogdch_cleanup_harvestsource,
        }

    # IClick

    def get_commands(self):
        return get_commands()


class OgdchAdminPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)

    # ------------------------------------------------------------
    # IActions
    # ------------------------------------------------------------
    # Allow adding of actions to the logic layer

    def get_actions(self):
        """
        Actions that are used by the commands.
        """
        return {
            "ogdch_reindex": admin.ogdch_reindex,
            "ogdch_check_indexing": admin.ogdch_check_indexing,
            "ogdch_check_field": admin.ogdch_check_field,
            "ogdch_latest_dataset_activities": admin.ogdch_latest_dataset_activities,
        }

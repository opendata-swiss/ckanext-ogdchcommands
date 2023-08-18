# coding=UTF-8

import ckan.plugins as plugins
import os
import logging
import ckanext.ogdchcommands.logic as l
import ckanext.ogdchcommands.admin_logic as admin
log = logging.getLogger(__name__)

__location__ = os.path.realpath(os.path.join(
    os.getcwd(),
    os.path.dirname(__file__))
)


class OgdchCommandsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)

    # ------------------------------------------------------------
    # IActions
    # ------------------------------------------------------------
    # Allow adding of actions to the logic layer

    def get_actions(self):
        '''
        Actions that are used by the commands.
        '''
        return {
            'ogdch_cleanup_harvestjobs': l.ogdch_cleanup_harvestjobs,
            'ogdch_cleanup_resources': l.ogdch_cleanup_resources,
            'ogdch_cleanup_filestore': l.ogdch_cleanup_filestore,
            'cleanup_package_extra': l.cleanup_package_extra,
            'ogdch_cleanup_harvestsource': l.ogdch_cleanup_harvestsource,
        }


class OgdchAdminPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)

    # ------------------------------------------------------------
    # IActions
    # ------------------------------------------------------------
    # Allow adding of actions to the logic layer

    def get_actions(self):
        '''
        Actions that are used by the commands.
        '''
        return {
            'ogdch_reindex': admin.ogdch_reindex,
            'ogdch_check_indexing': admin.ogdch_check_indexing,
            'ogdch_check_field': admin.ogdch_check_field,
            'ogdch_latest_dataset_activities': admin.ogdch_latest_dataset_activities, # noqa
        }

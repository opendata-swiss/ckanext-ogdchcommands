# coding=UTF-8

import ckan.plugins as plugins
import os
import logging
import ckanext.ogdchcommands.logic as l
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
            'ogdch_shacl_validate': l.ogdch_shacl_validate,
            'ogdch_cleanup_resources': l.ogdch_cleanup_resources,
            'ogdch_cleanup_harvestsource': l.ogdch_cleanup_harvestsource,
        }

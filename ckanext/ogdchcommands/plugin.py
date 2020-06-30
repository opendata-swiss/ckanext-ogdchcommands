# coding=UTF-8

import ckan.plugins as plugins
from ckan.lib.plugins import DefaultTranslation
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
        u'''
        Should return a dict, the keys being the name of the logic
        function and the values being the functions themselves.

        By decorating a function with the `ckan.logic.side_effect_free`
        decorator, the associated action will be made available by a GET
        request (as well as the usual POST request) through the action API.

        By decrorating a function with the 'ckan.plugins.toolkit.chained_action,
        the action will be chained to another function defined in plugins with a
        "first plugin wins" pattern, which means the first plugin declaring a
        chained action should be called first. Chained actions must be
        defined as action_function(original_action, context, data_dict)
        where the first parameter will be set to the action function in
        the next plugin or in core ckan. The chained action may call the
        original_action function, optionally passing different values,
        handling exceptions, returning different values and/or raising
        different exceptions to the caller.
        '''
        return {
            'ogdch_cleanup_harvestjobs': l.ogdch_cleanup_harvestjobs,
            'ogdch_shacl_validate': l.ogdch_shacl_validate,
        }

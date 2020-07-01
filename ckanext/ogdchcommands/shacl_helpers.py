import os
from ckan.exceptions import CkanConfigurationException
import ckan.plugins.toolkit as tk


def get_shacl_command_from_config():
    shacl_command = tk.config.get('ckanext.ogdchcommands.shacl_command_path')
    if not shacl_command:
        raise CkanConfigurationException(
            """'ckanext.ogdchcommands.shacl_command_path'
            setting is missing in config file""")
    return shacl_command


def get_shacl_resultsdir_from_config():
    shacl_results_dir = tk.config.get('ckanext.ogdchcommands.shacl_results_dir') # noqa
    if not shacl_results_dir:
        raise CkanConfigurationException(
            """'ckanext.ogdchcommands.shacl_results_dir'
            setting is missing in config file""")
    return shacl_results_dir


def get_shacl_shapesdir_from_config():
    shacl_shapesdir = tk.config.get('ckanext.ogdchcommands.shacl_shapes_dir') # noqa
    if not shacl_shapesdir:
        raise CkanConfigurationException(
            """'ckanext.ogdchcommands.shacl_shapes_dir'
            setting is missing in config file""")
    return shacl_shapesdir


def get_shacl_shape_file_path(filename):
    shapesdir = get_shacl_shapesdir_from_config()
    return os.path.join(shapesdir, filename)


def make_shacl_results_dir(harvest_source_id):
    resultdir = os.path.join(
        get_shacl_resultsdir_from_config(),
        harvest_source_id)
    try:
        os.makedirs(resultdir)
    except OSError:
        pass
    return resultdir


def get_shacl_file_path(resultdir, identifier, format):
    filename = '.'.join([identifier, format])
    filepath = os.path.join(resultdir, filename)
    return filepath


def get_shacl_result_file_path(resultdir, shapefile, format):
    identifier = shapefile.split('.')[0]
    filename = '.'.join(['result', identifier, format])
    filepath = os.path.join(resultdir, filename)
    return filepath

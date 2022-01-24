# encoding: utf-8

import logging
import json
import traceback
from ckan import authz
from ckan.lib.search import rebuild as rebuild_search_index
from ckan.lib.search import query_for
from ckan.plugins.toolkit import side_effect_free, get_or_bust
import ckan.model as model
import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)


@side_effect_free
def ogdch_reindex(context, data_dict):
    current_user = context.get('user')
    if not authz.is_sysadmin(current_user):
        return "not authorized"
    package_id = data_dict.get('id')
    only_missing = data_dict.get('only_missing')

    try:
        rebuild_search_index(package_id=package_id, only_missing=only_missing)
    except Exception as e:
        return {
            'msg': "an error occured",
            'error': str(e),
            'traceback': traceback.format_exc()
        }
    return "Success: search index was rebuilt"


@side_effect_free
def ogdch_check_indexing(context, data_dict):
    current_user = context.get('user')
    if not authz.is_sysadmin(current_user):
        return "not authorized"

    try:
        package_query = query_for(model.Package)

        log.debug("Checking packages search index...")
        pkgs_q = model.Session.query(model.Package).filter_by(
            state=model.State.ACTIVE)
        pkgs = set([pkg.id for pkg in pkgs_q])
        indexed_pkgs = \
            set(package_query.get_all_entity_ids(max_results=len(pkgs)))
        pkgs_not_indexed = pkgs - indexed_pkgs
        return ("there are {} packages not indexed"
                .format(len(pkgs_not_indexed)))
    except Exception:
        return "An Error occured"


@side_effect_free
def ogdch_check_field(context, data_dict):
    current_user = context.get('user')
    if not authz.is_sysadmin(current_user):
        return "not authorized"
    field = get_or_bust(data_dict, 'field')
    if not field:
        return "please provide a field name with field="

    results = []
    for package in _search_for_datasets(context):
        field_data_raw = package.get(field)
        field_data = ''
        if field_data_raw:
            try:
                field_data = json.loads(field_data_raw)
            except:
                pass
        result = {
            'name': package.get('name'),
            'field': field,
            'field_stored': field_data_raw,
            'field_data': field_data,
        }
        results.append(result)

    return {
        'msg': "So many datasets have been found:",
        'count': len(results),
        'pkgs': results,
    }


def _search_for_datasets(context):
    rows = 500
    page = 0
    result_count = 0
    fq = "dataset_type:(dataset)"
    processed_count = 0
    while page == 0 or processed_count < result_count:
        try:
            page = page + 1
            start = (page - 1) * rows
            data_dict = {
                'fq': fq,
                'rows': rows,
                'start': start,
                'include_private': True
            }
            result = tk.get_action('package_search')(context, data_dict)
            print("{} datasets have been found".format(result['count']))
            if not result_count:
                result_count = result['count']
            datasets_in_result = result.get('results')
            if datasets_in_result:
                for dataset in datasets_in_result:
                    yield dataset
            processed_count += len(datasets_in_result)
        except Exception as e:
            print("Error occured while searching for "
                  "packages with fq: {}, error: {}"
                  .format(fq, e))
            break

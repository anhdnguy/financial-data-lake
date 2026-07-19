from datetime import datetime
import re
from typing import List, Dict, Any

def _get_today():
    now = datetime.now()
    current_date = now.date()
    return current_date

def _convert_list_to_dict(data: List[Dict]) -> List[Dict]:
    result = {}

    for item in data:
        universe = item["universe"]
        ticker = item["ticker"]
        if universe not in result:
            result[universe] = [ticker]
        else:
            result[universe].append(ticker)
    
    return result

def _sanitize_symbol(symbol_list: Dict[str, List]) -> Dict[str, List]:
    for universe in symbol_list:
        symbol_list[universe] = [re.sub(r'[^a-zA-Z0-9\s]', '/', symbol) for symbol in symbol_list[universe]]
    
    return symbol_list

def _convert_tuple_to_list(data: Any) -> List:
    result = []
    for item in data:
        if isinstance(item, tuple) or isinstance(item, list):
            result.extend(list(item))
        elif isinstance(item, str):
            result.append(item)
        else:
            raise ValueError(f"Unknown type: {type(item)}")
    
    return result

def _sort_delisted_from_active(dict_symbols: Dict[str, Dict[str, List]]):
    result = {}

    _query = dict_symbols["query"]
    _import = dict_symbols["import"]

    for universe in _import:
        query_set = set(_query[universe])
        import_set = set(_import[universe])
        result[universe] = {}
        result[universe]["new_symbols"] = list(import_set - query_set)
        result[universe]["delisted_symbols"] = list(query_set - import_set)
    
    return result

def _list_to_tuple(list_of_str: List[str]):
    return [(s,) for s in list_of_str]

def _add_constants_to_tuple(list_tuple: List[tuple], tuple_constants: tuple):
    return [item + tuple_constants for item in list_tuple]
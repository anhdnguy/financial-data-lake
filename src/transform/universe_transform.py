from datetime import datetime
import re
from typing import List, Dict

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
        symbol_list[universe] = [re.sub(r'[^a-zA-Z0-9\s]', '.', symbol) for symbol in symbol_list[universe]]
    
    return symbol_list
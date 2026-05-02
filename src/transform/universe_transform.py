from datetime import datetime

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
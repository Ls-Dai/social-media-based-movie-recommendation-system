def db_get(info: dict) -> dict:
    """
    input:
        info: {
            'title': str, 
            'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
            'dates': list=[str],
        }
    output: 
        db_query_res: {
            'info': dict,
            'query_res': dict,
            'success': bool, 
        }

        query_res is a dict. Keys are dates, values are dicts. map(str -> {'pos': int, 'neg': int})

    """
    db_query_res = {
        'info': info, 
        'query_res': {
            '2000-01-01': {'pos': 0, 'neg': 0}, 
            '2000-01-02': {'pos': 0, 'neg': 0}, 
        },
        'success': True,
    }
    return db_query_res


def db_put(info: dict, model_outputs: dict) -> bool:
    return False 
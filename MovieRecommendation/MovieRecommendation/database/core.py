def db_get(info: dict, *args, **kwargs) -> list:
    """
    input:
        info: {
            'title': str, 
            'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
            'start_date': str,
            'end_date': str,
        }
    output: 
        A list result, including one score queried from database. If unsuccessful, return an empty list.
    """
    return [] 

def db_put(context: dict, *args, **kwargs) -> bool:
    """
    input:
        context: {
            "info": dict,
            "scores": list, 
        }
    output:
        A boolean value. IF OR NOT this operation was successful.
    """
    return None 
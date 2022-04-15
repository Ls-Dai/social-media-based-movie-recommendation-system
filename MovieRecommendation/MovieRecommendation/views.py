from django.http import HttpResponse
from django.shortcuts import render

# main methods
from database import db_get, db_put
from streaming import get_steaming_data
from algorithm import sentiment_analysis

def search(request):

    """
    info: {
        'title': str, 
        'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
        'start_date': str,
        'end_date': str,
    }
    """

    info = request.GET
    db_query_res = db_get(info=info)

    """
    context: {
        "info": dict,
        "scores": list, 
    }
    """

    if db_query_res:
        context = {"info": info, "scores": db_query_res, 'error_msg': ''}

    else:
        lines = get_steaming_data(info=info)
        scores = sentiment_analysis(lines=lines)
        context = {"info": info, "scores": scores, 'error_msg': ''}
        db_put(context=context)


    return render(
        request=request, 
        template_name='home.html', 
        context=context,
    )
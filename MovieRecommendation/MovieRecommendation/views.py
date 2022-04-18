from django.http import HttpResponse
from django.shortcuts import render

# main methods
from database import db_get, db_put
from streaming import get_steaming_data
from algorithm import process, postprocess


def search(request):
    """
    info: {
        'title': str, 
        'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
        'dates': list=[str],
    }
    """

    info = request.GET
    db_query_res = db_get(info=info)

    lines = get_steaming_data(info=db_query_res['info'])

    # sentiment analysis
    model_outputs = process(lines=lines)
    db_put(model_outputs)
    scores = postprocess(model_outputs=model_outputs, db_query_res=db_query_res)

    """
    context: {
        "info": dict,
        "scores": list, 
    }
    """
    context = {"info": info, "scores": scores, 'error_msg': ''}

    return render(
        request=request,
        template_name='home.html',
        context=context,
    )

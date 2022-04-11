from django.http import HttpResponse
from django.shortcuts import render


def search(request):
    query_dict = request.GET
    print(query_dict)
    title = query_dict['title']
    location = query_dict['location']
    time = query_dict['time']

    error_msg = ''

    results = {
        "title": title,
        "location": location,
        "time": time,
        "recommendation": 0
    }

    return render(
        request, 
        'home.html', 
        {'error_msg': error_msg, 'results':list(results.items())},
    )
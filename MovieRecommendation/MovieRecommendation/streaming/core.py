import tweepy 
import json
from datetime import datetime

def read_credentials(path="./MovieRecommendation/streaming/credentials/credentials.json"):
    with open(path, 'r') as f:
        credentials = json.load(f)
    return credentials

credentials = read_credentials()
twitter_credentials = credentials["Twitter"]
youtube_credentials = credentials["Youtube"]

CONSUMER_KEY = twitter_credentials.get("API Key", None)
CONSUMER_SECRET = twitter_credentials.get("API Key Secret", None)
ACCESS_TOKEN = twitter_credentials.get("Access Token", None) 
ACCESS_TOKEN_SECRET =  twitter_credentials.get("Access Token Secret", None)

AUTH = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH)


def get_tweets(info):

    title = info.get("title", None)
    geo_info = info.get("geo_info", None)
    dates = info.get("dates", None)

    print(30, "geo_info", geo_info)

    geo_code = '{},{},{}km'.format(geo_info['longitute'], geo_info['latitute'], geo_info['radius'])
    print(geo_code)

    text_list = []
    for tweet in tweepy.Cursor(
        API.search, 
        q=title, 
        count=1000, 
        geocode=geo_code, 
        lang="en", 
        # since=start_date, 
        # until=end_date
    ).items():
        tweet_id = tweet.id
        date = datetime.strptime(str(tweet.created_at)[:10], '%Y-%m-%d').strftime('%d-%m-%Y')
        text = tweet.text
        text_list.append(text)

    lines_dict = {}
    dates_num = len(dates)
    texts_num = len(text_list)
    for i, date in enumerate(dates):
        # print(i * int(texts_num / dates_num), (i+1) * int(texts_num / dates_num))
        lines_dict[date] = text_list[i * int(texts_num / dates_num): (i+1) * int(texts_num / dates_num)]
    return lines_dict

    


def get_steaming_data(info: dict) -> dict:
    """
    input:
        info: {
            'title': str, 
            'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
            'dates': ['2022-05-03'],
        }
    output: 
        
    lines_dict = {
        '2000-01-01': ["Hello.", "World"]
    }
    """
    lines_dict = get_tweets(info)

    return lines_dict 


if __name__ == '__main__':
    info = {
        'title': 'a', 
        'geo_info': {'longitute': 48.136353, 'latitute': 11.575004, 'radius': 1000000}, 
        'dates': ['2022-05-03', '2022-05-04'],
    }
    lines_dict = get_steaming_data(info)
    print(lines_dict)
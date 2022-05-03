import tweepy 
import json
from datetime import datetime

def read_credentials(path="./credentials/credentials.json"):
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
    start_date = info.get("start_date", None)
    end_date = info.get("end_date", None)

    geo_code = '{},{},{}km'.format(geo_info['longitute'], geo_info['latitute'], geo_info['radius'])

    text_list = []
    for tweet in tweepy.Cursor(
        API.search, 
        q=title, 
        count=10000, 
        geocode=geo_code, 
        lang="en", 
        # since=start_date, 
        # until=end_date
    ).items():
        tweet_id = tweet.id
        date = datetime.strptime(str(tweet.created_at)[:10], '%Y-%m-%d').strftime('%d-%m-%Y')
        text = tweet.text
        text_list.append(text)

    return text_list

    


def get_steaming_data(info: dict) -> dict:
    """
    input:
        info: {
            'title': str, 
            'geo_info': {'longitute': float, 'latitute': float, 'radius': float}, 
            'start_date': str,
            'end_date': str,
        }
    output: 
        
    lines_dict = {
        '2000-01-01': ["Hello.", "World"]
    }
    """
    get_tweets(info)
    lines_dict = None



    return lines_dict 


if __name__ == '__main__':
    info = {
        'title': 'Zootopia', 
        'geo_info': {'longitute': 48.136353, 'latitute': 11.575004, 'radius': 1000000}, 
        'start_date': "2022-01-01",
        'end_date': "2022-01-30",
    }
    lines_dict = get_steaming_data(info)
    print(lines_dict)
import tweepy 
import json
from datetime import datetime
import pyspark 
import re 
import random 


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


def string_cleaning(s: str):

    s = s.strip().lower()
    s = s.replace("&nbsp;", " ")
    s = re.sub(r'<br(\s\/)?>', ' ', s)
    s = re.sub(r' +', ' ', s)  # merge multiple spaces into one

    return s 


def steam_process_tweets(tweet_list, info):

    title = info.get("title", None)
    geo_info = info.get("geo_info", None)
    dates = info.get("dates", None)

    conf = pyspark.SparkConf("local").setAppName("part_3")
    sc = pyspark.SparkContext(conf=conf)

    rdd = sc.parallelize(tweet_list)

    # tranformation
    rdd = rdd.map(lambda x: (x['created_at'], x['text']))
    rdd = rdd.map(lambda x: (datetime.strptime(str(x[0])[:10], '%Y-%m-%d').strftime('%Y-%m-%d'), x[1]))
    rdd = rdd.filter(lambda x: x[0] in dates)
    rdd = rdd.map(lambda x: (x[0], [string_cleaning(x[1])]))
    rdd = rdd.reduceByKey(lambda x, y: x + y)
    
    # print(rdd.collect())
    lines_dict = dict(rdd.collect())
    # print(lines_dict)
    sc.stop()
    return lines_dict


def get_tweets(info):

    # tweet_attribute_list = ['author', 'contributors', 'coordinates', 'created_at', 'destroy', 'entities', 'favorite', 'favorite_count', 'favorited', 'geo', 'id', 'id_str', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status', 'lang', 'metadata', 'parse', 'parse_list', 'place', 'retweet', 'retweet_count', 'retweeted', 'retweeted_status', 'retweets', 'source', 'source_url', 'text', 'truncated', 'user']

    title = info.get("title", None)
    geo_info = info.get("geo_info", None)
    dates = info.get("dates", None)

    geo_code = '{},{},{}km'.format(geo_info['longitute'], geo_info['latitute'], geo_info['radius'])

    tweet_list = []
    # for tweet in random.choices(list(tweepy.Cursor(
    #     API.search, 
    #     wait_on_rate_limit=True, 
    #     q=title, 
    #     count=10000, 
    #     # geocode=geo_code, 
    #     lang="en", 
    #     # since=start_date, 
    #     # until=end_date
    # ).items(1000)), k=1000):
    for tweet in tweepy.Cursor(
        API.search, 
        wait_on_rate_limit=True, 
        q=title, 
        count=10000, 
        # geocode=geo_code, 
        lang="en", 
        # since=start_date, 
        # until=end_date
    ).items(1000):
        tweet_obj = {
            'author': tweet.author, 
            'contributors': tweet.contributors, 
            'coordinates': tweet.coordinates, 
            'created_at': tweet.created_at, 
            'destroy': tweet.destroy, 
            'entities': tweet.entities, 
            'favorite': tweet.favorite, 
            'favorite_count': tweet.favorite_count, 
            'favorited': tweet.favorited, 
            'geo': tweet.geo, 
            'id': tweet.id, 
            'id_str': tweet.id_str, 
            'in_reply_to_screen_name': tweet.in_reply_to_screen_name, 
            'in_reply_to_status_id': tweet.in_reply_to_status_id, 
            'in_reply_to_status_id_str': tweet.in_reply_to_status_id_str, 
            'in_reply_to_user_id': tweet.in_reply_to_user_id, 
            'in_reply_to_user_id_str': tweet.in_reply_to_user_id_str, 
            'is_quote_status': tweet.is_quote_status, 
            'lang': tweet.lang, 
            'metadata': tweet.metadata, 
            'parse': tweet.parse, 
            'parse_list': tweet.parse_list, 
            'place': tweet.place, 
            'retweet': tweet.retweet, 
            'retweet_count': tweet.retweet_count, 
            'retweeted': tweet.retweeted, 
            # 'retweeted_status': tweet.retweeted_status, 
            'retweets': tweet.retweets, 
            'source': tweet.source, 
            'source_url': tweet.source_url, 
            'text': tweet.text, 
            'truncated': tweet.truncated,
            'user': tweet.user,
        }
        
        tweet_list.append(tweet_obj)

    
    lines_dict = steam_process_tweets(tweet_list=tweet_list, info=info)
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
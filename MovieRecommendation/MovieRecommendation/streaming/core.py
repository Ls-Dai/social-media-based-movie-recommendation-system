import tweepy 
import json
from pyyoutube import Api
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
    
    lines_dict = dict(rdd.collect())
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
        # API.search, 
        API.search_tweets, 
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
        
    lines_dict = steam_process_tweets(tweet_list, info)
    return lines_dict


def filterFunc(hashtags):
    tmp = hashtags.split()
    ans = []
    for word in tmp:
        # word = word.lower()
        # print(word)
        if re.match("^[a-zA-Z0-9.,?!]*$", word):
            ans.append(word)
    # print(ans)
    return " ".join(ans)

def get_youtube(info):
    api = Api(api_key="AIzaSyBuSNBoLmIsLSk3kvnG3gx3-_vLQwSuzok")
    # title = "the batman"
    result = {}
    title = info.get("title", None)
    geo_info = info.get("geo_info", None)
    dates = info.get("dates", None)
    geo_code = '{}, {}'.format(geo_info['latitute'], geo_info['longitute'])
    # dates = ['2022-05-06', '2022-05-05', '2022-05-04', '2022-05-03', '2022-05-02', '2022-05-01', '2022-04-30', '2022-04-29', '2022-04-28', '2022-04-27', '2022-04-26', '2022-04-25', '2022-04-24', '2022-04-23', '2022-04-22', '2022-04-21', '2022-04-20', '2022-04-19', '2022-04-18', '2022-04-17', '2022-04-16', '2022-04-15', '2022-04-14', '2022-04-13', '2022-04-12', '2022-04-11', '2022-04-10', '2022-04-09', '2022-04-08', '2022-04-07']
    for date in dates:
        result[date] = []
    r = api.search(
        # location="43.2994285, -74.2179326000",
        location=geo_code,
        # location_radius="272624.9821146026m",
        location_radius=str(geo_info['radius'])+"m",
        q=title,
        parts=["snippet", "id"],
        count=50,
        published_after="2022-04-01T00:00:00Z",
        published_before="2022-05-01T00:00:00Z",
        safe_search="moderate",
        search_type="video")
    for line in r.items:
        # print(line.id.videoId, line.snippet.title)
        tmp_id = line.id.videoId
        tmp_video = api.get_video_by_id(video_id=tmp_id).items[0]
        # if(tmp_video.statistics.commentCount != 0):
        try:
            # print("---start---")
            ct_by_video = api.get_comment_threads(video_id=tmp_id, count=10)
            for ct in ct_by_video.items:
                raw_date = ct.snippet.topLevelComment.snippet.publishedAt
                datetime_tmp = datetime.strptime(str(raw_date)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
                tmp_co = ct.snippet.topLevelComment.snippet.textDisplay
                if datetime_tmp in result:
                    result[datetime_tmp].append(filterFunc(tmp_co))
                    # print(tmp_co, datetime_tmp)
            # print("---end---")
        except:
            # Something threw an error. Skip that video and move on
            print("has comments disabled, or something else went wrong")
    print(result) 
    return result
    
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
    # lines_dict = get_tweets(info)
    lines_dict = get_youtube(info)

    return lines_dict 


if __name__ == '__main__':
    info = {
        'title': 'a', 
        'geo_info': {'longitute': 48.136353, 'latitute': 11.575004, 'radius': 1000000}, 
        'dates': ['2022-05-03', '2022-05-04'],
    }
    lines_dict = get_steaming_data(info)
    print(lines_dict)
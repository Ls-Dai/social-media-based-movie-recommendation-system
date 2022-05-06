from pyyoutube import Api
import json
from datetime import datetime
import re
def filterFunc(hashtags):
    if re.match("^#[0-9a-z]+$", hashtags):
        return True
    else:
        return False

api = Api(api_key="AIzaSyCfwwJ_icd_hx32Qm2EI9Lb-q_eCk3EzMw")
title = "the batman"
result = {}
dates = ['2022-05-06', '2022-05-05', '2022-05-04', '2022-05-03', '2022-05-02', '2022-05-01', '2022-04-30', '2022-04-29', '2022-04-28', '2022-04-27', '2022-04-26', '2022-04-25', '2022-04-24', '2022-04-23', '2022-04-22', '2022-04-21', '2022-04-20', '2022-04-19', '2022-04-18', '2022-04-17', '2022-04-16', '2022-04-15', '2022-04-14', '2022-04-13', '2022-04-12', '2022-04-11', '2022-04-10', '2022-04-09', '2022-04-08', '2022-04-07']
for date in dates:
    result[date] = []
r = api.search(
    location="43.2994285, -74.2179326000",
    location_radius="272624.9821146026m",
    q=title,
    parts=["snippet", "id"],
    count=100,
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
        ct_by_video = api.get_comment_threads(video_id=tmp_id, count=100)
        for ct in ct_by_video.items:
            raw_date = ct.snippet.topLevelComment.snippet.publishedAt
            datetime_tmp = datetime.strptime(str(raw_date)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
            tmp_co = ct.snippet.topLevelComment.snippet.textDisplay
            if filterFunc(tmp_co) and datetime_tmp in result:
                result[datetime_tmp].append(tmp_co)
                print(tmp_co, datetime_tmp)
        # print("---end---")
    except:
        # Something threw an error. Skip that video and move on
        print("has comments disabled, or something else went wrong")

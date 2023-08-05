#by Ajay
#useful dict
from googleapiclient.discovery import build
import pandas as pd
from typing import List
category_data=dict({"1":"Film & Animation",
"2":"Autos & Vehicles",
"10":"Music",
"15":"Pets & Animals",
"17":"Sports",
"18":"Short Movies",
"19":"Travel & Events",
"20":"Gaming",
"21":"Videoblogging",
"22":"People & Blogs",
"23":"Comedy",
"24":"Entertainment",
"25":"News & Politics",
"26":"Howto & Style",
"27":"Education",
"28":"Science & Technology",
"29":"Nonprofits & Activism",
"30":"Movies",
"31":"Anime/Animation",
"32":"Action/Adventure",
"33":"Classics",
"34":"Comedy",
"35":"Documentary",
"36":"Drama",
"37":"Family",
"38":"Foreign",
"39":"Horror",
"40":"Sci-Fi/Fantasy",
"41":"Thriller",
"42":"Shorts",
"43":"Shows",
"44":"Trailers"})
total_no_of_playlists=2  #total no. of playlist you want to access from a perticular channel
def get_channel_details(youtube,channel_id):
    ch_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=channel_id
    )
    response = request.execute()
    for i in range(len(response['items'])):
        dataset=dict(channel_name=response['items'][i]['snippet']['title'],
                     channel_id= response['items'][i]['id'],
                     subscription_count=int(response['items'][i]['statistics']['subscriberCount']),
                     channel_views=response['items'][i]['statistics']['viewCount'],
                     channel_description=response['items'][i]['snippet']['description'],
                     playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                     channel_status = response['items'][i]['status']['privacyStatus'],
                     Channel_Type="")
    return dataset

def all_playlist_ids(youtube,channelId):
  page_available=True
  next_page_token=None
  playlist_id=[]
  playlist_name=[]
  while page_available:
    request = youtube.playlists().list(
          part="snippet,contentDetails",
          channelId=channelId,
          maxResults=50,
          pageToken=next_page_token
      )
    response = request.execute()
    for i in range(len(response['items'])):
      playlist_id.append(response['items'][i]['id'])
      playlist_name.append(response['items'][i]["snippet"]["localized"]["title"])
    next_page_token = response.get('nextPageToken')
    page_available=False if next_page_token is None else True
  return playlist_id,playlist_name


def get_video_ids(youtube, playlist_id):
    all_video_ids=[]
    page_available=True
    next_page_token=None
    while page_available:
      request = youtube.playlistItems().list(
          part='contentDetails',
          playlistId = playlist_id,
          maxResults = 50,
          pageToken=next_page_token)
      response = request.execute()
      for i in range(len(response['items'])):
        all_video_ids.append(response['items'][i]['contentDetails']['videoId'])
      next_page_token = response.get('nextPageToken')
      page_available=False if next_page_token is None else True
    return all_video_ids

def get_comment_details(youtube,single_video_id):
    comment_data = []
    page_available=True
    next_page_token=None
    while page_available:
      try:
        request = youtube.commentThreads().list(part="snippet,replies",
                                                videoId=single_video_id,
                                                maxResults=50,
                                                pageToken=next_page_token)
        response = request.execute()
      except:
        pass
      for i in range(len(response['items'])):
        data = dict(Comment_Id = response['items'][i]['id'],
                    Comment_Text = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_PublishedAt = response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])
        #datadict=dict()
        #datadict[f'Comment_Id_{count}']=data
        data['video_id']=single_video_id
        comment_data.append(data)
      next_page_token = response.get('nextPageToken')
      page_available=False if next_page_token is None else True

      count=1
      datadict=dict()
      for data in comment_data:
        datadict[f'Comment_Id_{count}']=data
        count+=1
    return datadict

def get_video_details_md(youtube, video_ids):
    video_details = []
    available_video_ids=[]
    for i in range(0, len(video_ids), 50):
      try:
        request = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        for video in response['items']:
          id_chnl=video['id']
          available_video_ids.append(video['id'])
          video_data = dict(Video_id=video['id'],
                            Video_Name = video['snippet']['title'],
                            Video_Description=video['snippet']['description'],
                            Tags=video['snippet'].get('tags'),
                            Published_Date = video['snippet']['publishedAt'],
                            Views_Count = int(video['statistics'].get('viewCount', 0)),
                            Likes_Count = int(video['statistics'].get('likeCount', 0)),
                            Dislikes_Count = int(video['statistics'].get('dislikeCount',0)),
                            Favorite_Count=int(video['statistics'].get('favoriteCount', 0)),
                            Comment_Count = int(video['statistics'].get('commentCount',0)),
                            Duration=video['contentDetails']['duration'],
                            Thumbnail=video['snippet']['thumbnails']['default']['url'],
                            caption_status=video['snippet'].get('caption', 'Not available'),
                            Category_type=category_data[video['snippet']['categoryId']]
                            )
          video_data['Comments']=get_comment_details(youtube,id_chnl)
          video_data['no_of_comments']=len(video_data['Comments'])
          video_details.append(video_data)
      except:
        pass
    return available_video_ids,video_details

def main_method(youtube, channel_id):
  channel_data=get_channel_details(youtube,channel_id)
  playlist_ids,playlist_names=all_playlist_ids(youtube,channel_id)
  channel_data['playlist_id']=playlist_ids
  channel_data['playlist_name']=playlist_names
  youtube_dataset=dict()
  youtube_dataset["Channel_Name"]=channel_data
  ply_lst_id=1
  playlist_count=1

  for key in playlist_ids:
    all_video_ids=get_video_ids(youtube, key)
    if(len(all_video_ids)<1):
      playlist_count-=1
      continue
    if(playlist_count>total_no_of_playlists):
      break
    playlist_count+=1
    available_video_ids,video_details = get_video_details_md(youtube,all_video_ids)
    video_detail_dict=dict()
    id=1
    video_category_details=[]
    video_detail_dict['playlist_id']=key
    video_detail_dict['playlist_name']=channel_data['playlist_name'][ply_lst_id-1]
    video_detail_dict['playlist_channel_id']=youtube_dataset['Channel_Name']['channel_id']
    video_detail_dict['no_of_videos']=len(available_video_ids)
    video_detail_dict['All_Video_ids']=available_video_ids
    for data in video_details:
      video_category_details.append(data['Category_type'])
      data['playlist_id']=key
      video_detail_dict[f'Video_Id_{id}']=data
      id+=1
    youtube_dataset[f'Playlist_Id_{ply_lst_id}']=video_detail_dict
    ply_lst_id+=1
  set1=set(video_category_details)
  type_label=""
  type_count=0
  for x in video_category_details:
      c1=video_category_details.count(x)
      if(type_count<c1):
          type_count=c1
          type_label=x
  youtube_dataset["Channel_Name"]["Channel_Type"]=type_label
  return youtube_dataset

#API key
api_key="AIzaSyC8FwWmyhw0zkKt2a-P5bxYDIjfR2kTxag"
youtube = build('youtube', 'v3', developerKey=api_key)
result=main_method(youtube, "UCek57Is95suO63uuW3iOGQw")
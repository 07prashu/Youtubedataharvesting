import re
import mysql.connector
from Youtube_Data_Extraction_Last import main_method,total_no_of_playlists
import Youtube_Data_Extraction_Last as yde
def mysql_connection():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="root@123",
        port='3306',
        database='youtube_datasets') ## old DB name is youtube_datasets
    return mydb
mydb=mysql_connection()
#mycursor.execute('show tables')
#for x in mycursor.fetchall():
#  print(x)

#result=main_method("UC2UXDak6o7rBm23k3Vv5dww") #data fatching from channel

#published data extraction
def pub_datetime(published_date):
  return ' '.join(published_date.rstrip('Z').split('T'))
#pub_datetime(published_date)

#duration extraction
def cal_duration(duration):
  temp_list=[int(x) for x in re.findall('\d+', duration)][::-1]
  cal_list=[1,60,3600,3600*24]
  duration_in_second=0
  for x,y in zip(temp_list,cal_list[:len(temp_list)]):
    duration_in_second+=x*y
  return duration_in_second
#cal_duration(duration)


def insert_data_in_tables(mydb,result):
    mycursor=mydb.cursor()#access mysql database
    #Channel Table data
    channel_uid=result['Channel_Name']['channel_id']  #primary key
    channel_name=result['Channel_Name']['channel_name']
    channel_type=result['Channel_Name']['Channel_Type']
    channel_views=result['Channel_Name']['channel_views']
    channel_description=result['Channel_Name']['channel_description']
    channel_status=result['Channel_Name']['channel_status']
    no_of_playlists=len(result['Channel_Name']['playlist_id'])
    #execute query to insert in channel table
    query="""INSERT INTO channel
    (channel_id,channel_name,channel_type,channel_views, channel_description,channel_status,no_of_playlists)
     SELECT %s, %s, %s, %s, %s, %s, %s  WHERE NOT exists (SELECT * FROM channel where channel_id = %s)"""
    field_values=(channel_uid,channel_name,channel_type,channel_views, channel_description,channel_status,no_of_playlists,channel_uid)
    mycursor.execute(query, field_values)
    print("I'm in channel")
    mydb.commit()

    #PlaylistTable Data
    if(yde.total_no_of_playlists>no_of_playlists):
        yde.total_no_of_playlists=no_of_playlists
    for i in range(yde.total_no_of_playlists):
        playlist_uid=result[f'Playlist_Id_{i+1}']['playlist_id'] #primary key
        playlist_name=result[f'Playlist_Id_{i+1}']['playlist_name']
        no_of_videos=result[f'Playlist_Id_{i+1}']['no_of_videos']
        channel_id=result['Channel_Name']['channel_id']   #foreign key
        query="""INSERT INTO playlist
        (playlist_id,playlist_name,no_of_videos,channel_id) select %s,%s,%s,%s
        WHERE NOT EXISTS (SELECT * FROM playlist WHERE playlist_id = %s)"""
        field_values=(playlist_uid,playlist_name,no_of_videos,channel_id,playlist_uid)
        mycursor.execute(query, field_values)
        mydb.commit()

    #video table Data
        for j in range(no_of_videos):
            video_uid=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_id'] #primary key
            video_name=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_Name']
            video_description=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_Description']
            published_date=pub_datetime(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Published_Date'])
            views_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Views_Count']
            likes_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Likes_Count']
            dislikes_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Dislikes_Count']
            favorite_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Dislikes_Count']
            comment_count=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comment_Count']
            duration=cal_duration(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Duration'])
            thumbnail=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Thumbnail']
            caption_status=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['caption_status']
            Category_type=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Category_type']
            no_of_comments=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['no_of_comments']
            video_playlist_id=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['playlist_id']  #foreign key
            query = """INSERT INTO video
            (video_id,video_name,video_description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status,Category_type,actual_comment_count,playlist_id)
             select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
             WHERE NOT EXISTS (SELECT * FROM video WHERE video_id = %s)"""
            field_values = (video_uid,video_name,video_description,published_date,views_count,likes_count,dislikes_count,favorite_count,comment_count,duration,thumbnail,caption_status,Category_type,no_of_comments,video_playlist_id,video_uid)
            mycursor.execute(query, field_values)
            mydb.commit()
    #Comment Table Data
            for k in range(no_of_comments):
                comment_uid=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Id'] #primary key
                comment_text=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Text']
                comment_author=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_Author']
                comment_published_date=pub_datetime(result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Comments'][f'Comment_Id_{k+1}']['Comment_PublishedAt'])
                video_id=result[f'Playlist_Id_{i+1}'][f'Video_Id_{j+1}']['Video_id']  #foreign key
                query = """INSERT INTO comment
                        (comment_id,comment_text,comment_author,comment_published_date,video_id) 
                        select %s,%s,%s,%s,%s
                        WHERE NOT EXISTS (SELECT * FROM comment WHERE comment_id = %s)"""
                field_values = (comment_uid,comment_text,comment_author,comment_published_date,video_id,comment_uid)
                mycursor.execute(query, field_values)
                mydb.commit()
#insert_data_in_tables(result) #insert complete channel data in mysql
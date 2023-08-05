import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from Youtube_Data_Extraction_Last import *
from database_connectivity_last import *
import pymongo
import matplotlib.pyplot as plt
import seaborn as sns
def project_decription():
    title_templ = """
        <h3 style="color:blue"><b>Project Description:</b></h3>
        """
    st.markdown(title_templ, unsafe_allow_html=True)
    data = f"""
                    **Project Name**: YouTube Data Harvesting and Warehousing

                    **Domain Name**: Social Media

                    **Used Technologies** : Python, SQL, MongoDB, Streamlit and Youtube API

                    It is a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

                    This project aims to develop a user-friendly Streamlit application that utilizes the  
                    Google API to extract information on a YouTube channel, stores it in a MongoDB
                    database, migrates it to a SQL data warehouse, and enables users to search for
                    channel details and join tables to view data in the Streamlit app.
                    """
    st.write(data)


def data_collection():
    title_templ = """
            <h3 style="color:green"><b>List of Channel Names & Channel Ids:</b></h3>
            """
    st.markdown(title_templ, unsafe_allow_html=True)
    channel_ids = ['UCLLw7jmFsvfIVaUFsLs8mlQ',
                   'UCiT9RITQ9PW6BhXK0y2jaeg',
                   'UC7cs8q-gJRlGwj4A8OmCmXg',
                   'UC2UXDak6o7rBm23k3Vv5dww',
                   'UCrY1Ro4UXwMib9Qug3eJNWA',
                   'UCV8e2g4IWQqK71bbzGDEI4Q',
                   'UCq6XkhO5SZ66N04IcPbqNcw',
                   'UC6GcGLJYWEblBjwt7Q_ouyw',
                   'UCek57Is95suO63uuW3iOGQw',
                   'UCJublDh2UsiIKsAE1553miw'
                   ]
    channel_name = ['Luke Barousse',
                    'Ken Jee',
                    'Alex The Analyst',
                    'Tina Huang',
                    'Kahan Data Solutions',
                    'Data Professor',
                    'Keith Galli',
                    'Anthony Ward',
                    'Russell Peters',
                    'Greg Hogg']
    data = pd.concat([pd.Series(channel_name, name="Channel_Names"), pd.Series(channel_ids, name='Channel_Ids')],
                     axis=1)
    st.dataframe(data, use_container_width=True)


def main():
    title_templ = """
    <div style="background-color:#541E1B;padding:8px;">
    <h2 style="color:white;padding:15px;">YouTube Data Harvesting and Warehousing</h2></div>
    """
    st.markdown(title_templ, unsafe_allow_html=True)
    with st.sidebar:
        selected = option_menu(
            menu_title="Project Menu",
            options=["About the Project", "Data Collection", "Select and Store", "Migration of Data", "Data Analysis"],
            default_index=0
        )
    if selected == "About the Project":
        project_decription()
    elif selected == "Data Collection":
        data_collection()
    elif selected == "Select and Store":
        st.write('')
        col1, col2 = st.columns(2)
        with col1:
            channel_id = st.text_input("**Enter Channel Id:**", value="UC-HLXw5cFC-7zqaXqTIlj-g")
        with col2:
            youtube_api_key = st.text_input("**Enter Youtube Api Key:**",value="AIzaSyDkvnnt67F9tMvkFPbsqzckqPSHmgZ5pQo")
        store_button = st.button('Store data in MongoDB 👈')
        if (store_button):
            client = pymongo.MongoClient('mongodb://localhost:27017')
            api_key = youtube_api_key
            youtube = build('youtube', 'v3', developerKey=api_key)
            result=main_method(youtube,channel_id)
            #with open('youtube_data.json','w') as f:
            #    json.dump(result,f)

            db=client['youtube_datasets']
            collection = db['data']
            collection.insert_one(result)
            st.success("Successfully Data Stored in MongoDB")

    elif selected == "Migration of Data":
        channel_name = st.text_input("Enter Channel Name", value="soha channel")
        migrate_button = st.button('Migrate data for Channel 👈')
        if (migrate_button):
            mysql1 = mysql_connection()
            client = pymongo.MongoClient('mongodb://localhost:27017')
            db = client['youtube_datasets']
            collection = db['data']
            #collection=collection['youtube_test']
            for result in collection.find({"Channel_Name.channel_name":channel_name}):
                insert_data_in_tables(mysql1,result)
            st.success("Successfully Data Migrated in Mysql")

    else:
        option_list = ['1. What are the names of all the videos and their corresponding channels?',
                       '2. Which channels have the most number of videos, and how many videos do they have?',
                       '3. What are the top 10 most viewed videos and their respective channels?',
                       '4. How many comments were made on each video, and what are their corresponding video names?',
                       '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                       '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                       '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                       '8. What are the names of all the channels that have published videos in the year 2022?',
                       '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                       '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
        try:
            option = st.sidebar.selectbox('Select Question', option_list)
            mysql1 = mysql_connection()
            p = mysql1.cursor()
            if option == option_list[0]:
                p.execute(
                    "select v.video_name as 'Video Name', ch.channel_name as 'Channel Name' from video as v inner join playlist as p on p.playlist_id=v.playlist_id inner join channel as ch on ch.channel_id=p.channel_id")
                myresult = p.fetchall()
                video_name = []
                channel_name = []
                for x in myresult:
                    video_name.append(x[0])
                    channel_name.append(x[1])
                data = pd.concat(
                    [pd.Series(video_name, name="Video Names"), pd.Series(channel_name, name='Channel Name')], axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[1]:
                p.execute("""select ch.channel_name,count(v.video_id) from video v
                 inner join playlist as p on v.playlist_id=p.playlist_id 
                 inner join channel as ch on p.channel_id=ch.channel_id group by(ch.channel_id) order by (count(v.video_id)) desc""")
                myresult = p.fetchall()
                no_of_videos = []
                channel_name = []
                for x in myresult:
                    channel_name.append(x[0])
                    no_of_videos.append(x[1])
                data = pd.concat([pd.Series(channel_name[0], name="Channel Names"),
                                  pd.Series(no_of_videos[0], name='Total No. of Videos')], axis=1)
                st.dataframe(data, use_container_width=True)
                st.write("**Pie chart for Channel Names & respective videos:**")
                fig, ax1 = plt.subplots()
                ax1.pie(no_of_videos, labels=channel_name, autopct='%1.2f%%',shadow=True)
                ax1.axis('equal')  # Equal aspect ratio to ensure that pie is drawn as a circle
                st.pyplot(fig)
            elif option == option_list[2]:
                p.execute(
                    "select ch.channel_name ,v.video_id,v.video_name ,v.view_count from video as v inner join playlist as p on p.playlist_id=v.playlist_id inner join channel as ch on ch.channel_id=p.channel_id order by(v.view_count) DESC LIMIT 10")
                myresult = p.fetchall()
                channel_name = []
                video_ids = []
                video_names = []
                view_count = []
                for x in myresult:
                    channel_name.append(x[0])
                    video_ids.append(x[1])
                    video_names.append(x[2])
                    view_count.append(x[3])
                data = pd.concat(
                    [pd.Series(channel_name, name="Channel Names"), pd.Series(video_ids, name='Video Ids'),
                     pd.Series(video_names, name='Video Names'), pd.Series(view_count, name='View Counts')],
                    axis=1)
                st.dataframe(data, use_container_width=True)
                st.write("**Pie chart for video Name and respective View Count:**")
                fig, ax1 = plt.subplots()
                ax1.pie(view_count, labels=video_names, shadow=True)
                ax1.axis('equal')  # Equal aspect ratio to ensure that pie is drawn as a circle
                st.pyplot(fig)

                st.write("**Bar chart for video Name and respective View Count:**")
                data_set=pd.concat([pd.Series(video_names, name='video_name'),pd.Series(view_count, name='view_count'),pd.Series(channel_name, name='channel_name')],axis=1)
                st.bar_chart(data_set,x='video_name',y='view_count')



            elif option == option_list[3]:
                p.execute("select video_name ,actual_comment_count from video")
                myresult = p.fetchall()
                video_names = []
                comment_count = []
                for x in myresult:
                    video_names.append(x[0])
                    comment_count.append(x[1])
                data = pd.concat(
                    [pd.Series(video_names, name="Video Names"), pd.Series(comment_count, name='No. of Comments')],
                    axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[4]:
                p.execute("""select max(v.like_count),ch.channel_name from video as v
                 inner join playlist as p on p.playlist_id=v.playlist_id inner join channel as ch
                  on ch.channel_id=p.channel_id group by (ch.channel_id)""")
                myresult = p.fetchall()
                channel_names = []
                video_names = []
                video_ids = []
                like_count = []
                for x in myresult:
                    channel_names.append(x[1])
                    like_count.append(x[0])
                for x,y in zip(channel_names,like_count):
                    p.execute("""select v.video_name,v.video_id from video v
                     inner join playlist as p on v.playlist_id=p.playlist_id inner join channel as ch
                      on p.channel_id=ch.channel_id where ch.channel_name = %s and v.like_count = %s""",(x, y))
                    myresult = p.fetchall()
                    for x in myresult:
                        video_names.append(x[0])
                        video_ids.append(x[1])
                data = pd.concat(
                    [pd.Series(channel_names, name='Channel Names'), pd.Series(video_names, name="Video Names"),
                     pd.Series(video_ids, name="Video Ids"), pd.Series(like_count, name="Like Counts")], axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[5]:
                p.execute("select v.video_name, (v.like_count+v.dislike_count) from video as v")
                myresult = p.fetchall()
                video_names = []
                like_dislike_count = []
                for x in myresult:
                    video_names.append(x[0])
                    like_dislike_count.append(x[1])
                data = pd.concat([pd.Series(video_names, name="Video Names"),
                                  pd.Series(like_dislike_count, name='Total Like-Dislike')], axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[6]:
                p.execute("select channel_name, channel_views from channel")
                myresult = p.fetchall()
                channel_names = []
                channel_views = []
                for x in myresult:
                    channel_names.append(x[0])
                    channel_views.append(x[1])
                data = pd.concat(
                    [pd.Series(channel_names, name="Channel Names"), pd.Series(channel_views, name='Total Views')],
                    axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[7]:
                p.execute("""select distinct(ch.channel_name) from video as v 
                inner join playlist as p on p.playlist_id=v.playlist_id 
                inner join channel as ch on ch.channel_id=p.channel_id where year(published_date)=2022""")
                myresult = p.fetchall()
                channel_names = []
                for x in myresult:
                    channel_names.append(x[0])
                data = pd.Series(channel_names, name="Channel Names")
                st.dataframe(data, use_container_width=True)
            elif option == option_list[8]:
                p.execute("""select ch.channel_name, avg(v.duration) from video as v
                inner join playlist as p on p.playlist_id=v.playlist_id 
                inner join channel as ch on ch.channel_id=p.channel_id group by (ch.channel_id)""")
                myresult = p.fetchall()
                channel_names = []
                duration = []
                for x in myresult:
                    channel_names.append(x[0])
                    duration.append(x[1])
                data = pd.concat([pd.Series(channel_names, name="Channel Names"),
                                  pd.Series(duration, name=' Avg. Duration (in Seconds)')], axis=1)
                st.dataframe(data, use_container_width=True)
            elif option == option_list[9]:
                p.execute("""select ch.channel_name,max(v.actual_comment_count) from video as v 
                inner join playlist as p on p.playlist_id=v.playlist_id inner join channel as ch
                 on ch.channel_id=p.channel_id group by (ch.channel_id)""")
                myresult = p.fetchall()
                channel_names = []
                comnt_count = []
                for x in myresult:
                    channel_names.append(x[0])
                    comnt_count.append(x[1])
                video_names = []
                video_id = []
                for ch_nm, cm_cnt in zip(channel_names, comnt_count):
                    p.execute("""select v.video_name,v.video_id from video v
                     inner join playlist as p on v.playlist_id=p.playlist_id inner join channel as ch
                      on p.channel_id=ch.channel_id where ch.channel_name = %s and v.actual_comment_count = %s""",(ch_nm, cm_cnt))
                    myresult = p.fetchall()
                    for x in myresult:
                        video_names.append(x[0])
                        video_id.append(x[1])
                data = pd.concat([pd.Series(channel_names, name="Channel Names"), pd.Series(video_id, name="Video Ids"),
                                  pd.Series(video_names, name="Video Name"),pd.Series(comnt_count, name="Comment count")], axis=1)
                st.dataframe(data, use_container_width=True)
        except:
            pass
            #print('something wrong')


main()

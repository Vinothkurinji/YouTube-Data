import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
from PIL import Image
import pymongo
import psycopg2
import isodate
import sqlalchemy
from sqlalchemy import create_engine

#Youtube API Connector
youtube = build('youtube', 'v3', 
                developerKey='API-KEY')

#MongoDB Connector  
client=pymongo.MongoClient("mongodb+srv://vinothkurinji:vinothkurinjim@cluster0.kwmaxd2.mongodb.net/?retryWrites=true&w=majority")
db=client["Project_youtube"]
collection=db.channelDetails
#MySQL Connector


conn = psycopg2.connect(
    host="localhost",
    user="root",
    password="vinoth",
    database="Project_youtube"
)
cursor = conn.cursor()
engine = create_engine('postgresql://localhost:root@localhost:5432/Project_youtube')

#Pushing Data to MongoDB
def push_to_mongo(pd_youtube):
 push_status = collection.insert_one(pd_youtube)
 return push_status

#Getting Channel name:
def channel_names():   
    ch_name = []
    for i in collection.find():
        ch_name.append(i['data']['channel_name'])
    return ch_name

#Getting Channel Details:
def get_channel_details(channelid):
    ch_data={}
    playlist_dict={}
    #--------------------------------Getting Basic Channel details----------------------------------------------
    ch_response = youtube.channels().list(
    part='snippet,contentDetails,statistics,status',
    id=channelid).execute()
    channelName=ch_response['items'][0]['snippet']['title']
    Playlist_id =ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    for i in range(len(ch_response['items'])):
     ch_data[channelName] = {
        "channel_id":ch_response['items'][i]['id'],
        "channel_name":ch_response['items'][i]['snippet']['title'],
        "Subscribers" : ch_response['items'][i]['statistics']['subscriberCount'],
        "video_count":ch_response['items'][i]['statistics']['videoCount'],
        "channel_views":ch_response['items'][i]['statistics']['viewCount'],
        "channel_description":ch_response['items'][i]["snippet"]["description"],
        "channel_status":ch_response['items'][i]['status']['privacyStatus'],
        "Playlist":{},
        "Videos":{},
        "Comments":{}
        }
     #----------------------------------Getting all PlayList details of the  Channel------------------------------------
    playlist_Response = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channelid,
        maxResults=25
    ).execute()
    for i in range (len(playlist_Response['items'])):
   
         ch_data[channelName]["Playlist"][playlist_Response['items'][i]['id']]=dict(
            playlist_id= playlist_Response['items'][i]['id'],
            channel_id=playlist_Response['items'][i]['snippet']['channelId'],
            playlist_title= playlist_Response['items'][i]['snippet']['title'],
            videos=[]
        )
    # ---------------------------------------------all Video Details of the channel-----------------------------------------------------
    for i in [i['id'] for i in playlist_Response['items']]:
        if i not in playlist_dict:
            playlist_dict[i] =  youtube.playlistItems().list(part="contentDetails",playlistId=i,maxResults=5).execute()
    for key, val in playlist_dict.items():
        for videos in val['items']: 
           ch_data[channelName]["Playlist"][key]["videos"] += [videos['contentDetails']['videoId']]
    for key, val in playlist_dict.items():
        for i in val['items']:
            vid_dict = {}
            if i['contentDetails']['videoId'] not in ch_data[channelName]["Videos"]:
                video_details = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=i['contentDetails']['videoId']).execute()
                if len(video_details['items']) > 0:
                    video_dict = video_details['items'][0]
                    vid_dict["video_id"] = i['contentDetails']['videoId']
                    vid_dict["channel_id"] = channelid
                    vid_dict["video_name"] = video_dict['snippet']['title']
                    vid_dict["video_description"] = video_dict['snippet']['description']
                    vid_dict["published_at"] = video_dict['snippet']['publishedAt'].replace("T"," ").replace("Z","")
                    vid_dict["view_count"] = video_dict['statistics']['viewCount']
                    vid_dict["like_count"] = video_dict['statistics']['likeCount']
                    vid_dict["channel_name"] = channelName
                    vid_dict["comment_count"] = video_dict['statistics']['commentCount']
                    vid_dict["duration"] = isodate.parse_duration(video_dict['contentDetails']['duration']).total_seconds()
                    vid_dict["thumbnail"] = video_dict['snippet']['thumbnails']['default']['url']
                    vid_dict["caption_status"] = video_dict['contentDetails']['caption']
                    vid_dict["comments"] = []
                    ch_data[channelName]["Videos"][i['contentDetails']['videoId']] = vid_dict
#--------------------------------------------------------Comment Details------------------------------------------------------------
#--------------------------------------------------------Comment Details------------------------------------------------------------
    for video_id in ch_data[channelName]["Videos"]:
        com_dict = {}
        comment_dict = youtube.commentThreads().list(part="snippet,replies",
                    videoId=video_id,maxResults=15).execute()
        for comment in comment_dict['items']:
            video_id = comment['snippet']['videoId']
            comment_id = comment['snippet']['topLevelComment']['id']
            ch_data[channelName]["Videos"][video_id]["comments"] += [comment_id]
            com_dict["channel_id"] = channelid
            com_dict["Video_id"] = video_id
            com_dict["Comment_Id"] = comment_id
            com_dict["Comment_Text"] = comment['snippet']['topLevelComment']['snippet']['textOriginal']
            com_dict["Comment_Author"] = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
            com_dict["Comment_PublishedAt"] = comment['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T"," ").replace("Z","")
            ch_data[channelName]["Comments"][comment_id] = com_dict

    return {"Channel_Name":ch_data[channelName]['channel_name'],'data':ch_data[channelName]}   
#----------------------------------------------------------MYSQL Migration----------------------------------------------------------------
def migrate_to_sql(channel_name):
    channel_data = collection.find({"Channel_Name": channel_name})[0]

    channel_df = pd.DataFrame([[channel_data["data"]["channel_name"], channel_data["data"]["channel_id"],
                                channel_data["data"]["channel_views"], channel_data["data"]["channel_description"],
                                channel_data["data"]["video_count"], channel_data["data"]["channel_status"]]],
                              columns=["Channel_Name", "Channel_Id", "Channel_Views", "Channel_Description",
                                       "Total_videos", "Channel_status"])

    channel_df.to_sql('channel', engine, if_exists='append', index=False,
                      dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Channel_Views": sqlalchemy.types.BigInteger,
                             "Channel_Description": sqlalchemy.types.TEXT})
    playlist = []
    for key, val in channel_data["data"]["Playlist"].items():
        playlist.append([key, val["channel_id"], val["playlist_title"]])
    playlist_df = pd.DataFrame(playlist, columns=["Playlist_Id", "Channel_Id", "Playlist_name"])
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                       dtype={"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                              "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                              "Playlist_name": sqlalchemy.types.VARCHAR(length=225)})

    video = []
    for key, val in channel_data["data"]["Videos"].items():
        video.append([key, val['channel_id'], val["video_name"], val["channel_name"],val["video_description"], val["published_at"], val["view_count"], val["like_count"], val["comment_count"], val["duration"], val["caption_status"]])
    video_df = pd.DataFrame(video, columns=["video_id", "channel_id","video_name","channel_name", "video_description", 'published_date', 'view_count', 'like_count', 'comment_count', 'duration', 'caption_status'])
    video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype={'video_id': sqlalchemy.types.VARCHAR(length=225),
                           'channel_id': sqlalchemy.types.VARCHAR(length=225),
                           'channel_name': sqlalchemy.types.VARCHAR(length=225),
                           'video_name': sqlalchemy.types.VARCHAR(length=225),
                           'video_description': sqlalchemy.types.TEXT,
                           'published_date': sqlalchemy.types.String(length=50),
                           'view_count': sqlalchemy.types.BigInteger,
                           'like_count': sqlalchemy.types.BigInteger,
                        #    'dislike_count': sqlalchemy.types.Integer,
                           'comment_count': sqlalchemy.types.Integer,
                           'duration': sqlalchemy.types.VARCHAR(length=1024),
                           'caption_status': sqlalchemy.types.VARCHAR(length=225)})
    # video_df['durationSecs'] = video_df['duration'].apply(lambda x: isodate.parse_duration(x).total_seconds())
    comment = []
    for key, val in channel_data["data"]["Comments"].items():
        comment.append([val["Video_id"],key, val["Comment_Text"], val["Comment_Author"], val["Comment_PublishedAt"]])
    comment_df = pd.DataFrame(comment, columns=['Video_id', 'Comment_Id', 'Comment_Text', 'Comment_Author', 'Comment_Published_date'])
    comment_df.to_sql('comments', engine, if_exists='append', index=False,
                      dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            #  'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Text': sqlalchemy.types.TEXT,
                             'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                             'Comment_Published_date': sqlalchemy.types.String(length=50)})
    return 0
#----------------------------------------------------Streamlit Design-----------------------------------------------------------------------
icon = Image.open("Dataimg.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Shiva Kumar",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded")
                
with st.sidebar:
    selected = option_menu(None,
                           options=["Home","Extract and Store","Insights"],
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0)
if selected=="Home":
    st.title('You:red[Tube]  Data :red[Harvesting] and :red[Warehousing]')
    st.markdown("In This Project we will get YouTube Channel data from YouTube API with the help of 'Channel ID' , Then We Will Store the channel data into Mongo DB Atlas as a Document then we will store the data in MySQL Database for Data Analysis. This Entire Project depends on Extract Transform Load Process(ETL)")
    # st.subheader("Welcome :orange[Tech Geeks] Here :orange[Praveen] ️")    
elif selected=="Extract and Store":
    extract,tranfer=st.tabs(["Extract Data","Tranfer Data"])
#Extract Button Functionalities
    with extract:
        st.title("Enter the Channel id")
        ch_id=st.text_input("")
        srch_btn =st.button("Search")
        upld_button=st.button("Upload to MongoDB")
        if ch_id and srch_btn:
            with st.spinner('Please Wait a sec....Retreiving your channel details.........'):
             retrieved_data=get_channel_details(ch_id)
             channel_table={"Channel Name":retrieved_data['Channel_Name'],"Channel id":retrieved_data['data']['channel_id'],"Description":retrieved_data['data']['channel_description'],"Subscribers":retrieved_data['data']['Subscribers'],"No of Videos":retrieved_data['data']['video_count'],"Channel Status":retrieved_data['data']['channel_status']}
             st.table(channel_table)
        if upld_button:
            cursor = collection.find()
            ds=[item["data"]["channel_id"] for item in cursor]
            if ch_id in ds:
                st.error('The Channel Data already Exists!!', icon="🚨")
                #st.write("oops!it's an existing data")   
            else:   
                channel_info = get_channel_details(ch_id)
                pushed_to_mongo = push_to_mongo(channel_info)
                if pushed_to_mongo.acknowledged:
                    st.success('Data has been Inserted Successfully!',icon="✅")
                else:
                    st.error('DATA NOT INSERTED IN MONGODB', icon="🚨")
#Tranfer button functionlities
    with tranfer:
        st.title("Select the Channel")
        ch_name=channel_names()
        channel_name=st.selectbox(label="Select channel to Add the selected channel details MYSQL Database",options=ch_name)
        migrate_button=st.button("migrate")
        if migrate_button:
            # try:
             migrate_to_sql(channel_name)
             st.success('Data has been successfully migrated to SQL DB',icon="✅")
            # except: UCLImLUyuH9hJwTGdn4N68rg
            #     st.error("Channel details already Exists !!",icon="🚨")
        
elif selected=="Insights":
    st.write("Select any question from below list to provide insights")
    questions = st.selectbox(label='Questions',options=
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name
                            FROM video
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                            FROM channel
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views 
                            FROM video
                            ORDER BY view_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name As Video_Name,comment_count As Total_Comments FROM video""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name As channel_Name,video_name AS Video_Name,like_count As Total_likes FROM video order by view_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Video_Name,like_count As Total_likes FROM video order by view_count ASC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,channel_views As Total_Views FROM Channel""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name As Channel_Name,video_name As Video_Name,published_date As Published_Date FROM video where published_date>='2022-01-01 00:00:00' and published_date<='2023-01-01 00:00:00' order by published_date ASC """)
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name As Channel_Name,avg(duration) As Average_Duration_In_Seconds from video group by channel_name;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name As Channel_Name,video_name As Video_Name,comment_count As Total_No_Of_Comments from video order by comment_count Desc;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        df.index+=1
        st.write(df)

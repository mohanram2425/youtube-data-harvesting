from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime
import re

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mohan",
    database="youtube"
)
cursor = conn.cursor()



api='AIzaSyBVetwF6C48eld9ZqUOL1pkrTdAqRu2N5E'
def Api_connect():
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        channel_data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return channel_data

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=5
            )
            response=request.execute()
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)           
    except:
        pass
    return Comment_data

def get_playlist_details(channel_id):
    next_page_token = None
    all_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get('items', []): 
            data = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'Channel_Id': item['snippet']['channelId'],
                'Channel_Name': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount']
            }
            all_data.append(data)
        next_page_token = response.get('nextPageToken')  
        if not next_page_token:
            break
    return all_data

def create_channels_table(conn):
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS channels (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80) PRIMARY KEY,
                        Subscribers BIGINT,
                        Views BIGINT,
                        Total_Videos INT,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(80)
                    )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def insert_channel_details(conn,channel_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO channels (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                      VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    cursor.execute(insert_query, (
        channel_data['Channel_Name'],
        channel_data['Channel_Id'],
        channel_data['Subscribers'],
        channel_data['Views'],
        channel_data['Total_Videos'],
        channel_data['Channel_Description'],
        channel_data['Playlist_Id']
    ))
    conn.commit()
    cursor.close()

def create_playlists_table(conn):
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                        Playlist_Id VARCHAR(100) PRIMARY KEY,
                        Title VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Channel_Name VARCHAR(100),
                        PublishedAt TIMESTAMP,
                        Video_Count INT
                    )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def create_videos_table(conn):
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS videos (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Video_Id VARCHAR(30) PRIMARY KEY,
                        Title VARCHAR(150),
                        Tags TEXT,
                        Thumbnail VARCHAR(200),
                        Description TEXT,
                        Published_Date TIMESTAMP,
                        Duration INT,
                        Views BIGINT,
                        Likes BIGINT,
                        Comments INT,
                        Favorite_Count INT,
                        Definition VARCHAR(10),
                        Caption_Status VARCHAR(50)
                    )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def create_comments_table(conn):
    cursor = conn.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS comments (
                        Comment_Id VARCHAR(100) PRIMARY KEY,
                        Video_Id VARCHAR(50),
                        Comment_Text TEXT,
                        Comment_Author VARCHAR(150),
                        Comment_Published TIMESTAMP
                    )'''
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def insert_playlist_details(conn, playlist_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    for playlist_item in playlist_data:
        published_at = datetime.strptime(playlist_item['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(insert_query, (
            playlist_item['Playlist_Id'],
            playlist_item['Title'],
            playlist_item['Channel_Id'],
            playlist_item['Channel_Name'],
            published_at,  
            playlist_item['Video_Count']
        ))
    conn.commit()
    cursor.close()


def duration_to_minutes(duration_str):
    match = re.match(r'PT(\d+)M(\d+)S', duration_str)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        tt= minutes + seconds/60
        return tt
    return 0

def insert_video_details(conn, video_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    for video_item in video_data:
        published_date = datetime.strptime(video_item['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        duration_seconds = duration_to_minutes(video_item['Duration'])
        video_item['Published_Date'] = published_date

        cursor.execute(insert_query, (
            video_item['Channel_Name'],
            video_item['Channel_Id'],
            video_item['Video_Id'],
            video_item['Title'],
            ','.join(video_item['Tags']),
            video_item['Thumbnail'],
            video_item['Description'],
            video_item['Published_Date'],
            duration_seconds,
            int(video_item['Views']),
            int(video_item['Likes']),
            int(video_item['Comments']),
            int(video_item['Favorite_Count']),
            video_item['Definition'],
            video_item['Caption_Status']
        ))
    conn.commit()
    cursor.close()


def insert_comment_details(conn, comment_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                      VALUES (%s, %s, %s, %s, %s)'''
    for comment_item in comment_data:
        comment_published = datetime.strptime(comment_item['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(insert_query, (
            comment_item['Comment_Id'],
            comment_item['Video_Id'],
            comment_item['Comment_Text'],
            comment_item['Comment_Author'],
            comment_published 
        ))
    conn.commit()
    cursor.close()

create_videos_table(conn)
create_playlists_table(conn)
create_channels_table(conn)
create_comments_table(conn)
                      

def insert_data(channel_id):
    channel_data=get_channel_info(channel_id)
    playlist_data=get_playlist_details(channel_id)
    video_Ids=get_videos_ids(channel_id)
    video_data=get_video_info(video_Ids)
    comment_data=get_comment_info(video_Ids)
    
    insert_channel_details(conn,channel_data)
    insert_playlist_details(conn, playlist_data)
    insert_video_details(conn, video_data)
    insert_comment_details(conn, comment_data)

def channel_input():
    st.title("Input Channel IDs")
    channel_ids = st.text_area("Enter Channel IDs (one per line):")
    if st.button("Submit"):
        channel_ids = channel_ids.split('\n') 
        st.write("Channel IDs submitted:", channel_ids)
        for channel_id in channel_ids:
            insert_data(channel_id)


def select_questions(): 
    question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
    if question == "1. All the videos and the channel name":
        query1 = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
        cursor.execute(query1)
        t1 = cursor.fetchall()
        df = pd.DataFrame(t1, columns=["video title", "channel name"])
        st.write(df)

    elif question == "2. channels with most number of videos":
        query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels 
                ORDER BY total_videos DESC'''
        cursor.execute(query2)
        t2 = cursor.fetchall()
        df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
        st.write(df2)

    elif question == "3. 10 most viewed videos":
        query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos 
                WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
        cursor.execute(query3)
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
        st.write(df3)

    elif question == "4. comments in each videos":
        query4 = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
        cursor.execute(query4)
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
        st.write(df4)

    elif question == "5. Videos with highest likes":
        query5 = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount
                    FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
        cursor.execute(query5)
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
        st.write(df5)

    elif question == "6. likes of all videos":
        query6 = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
        cursor.execute(query6)
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
        st.write(df6)

    elif question == "7. views of each channel":
        query7 = '''SELECT channel_name AS channelname, views AS totalviews FROM channels'''
        cursor.execute(query7)
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
        st.write(df7)

    elif question == "8. videos published in the year of 2022":
        query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos
                    WHERE YEAR(published_date) = 2022'''
        cursor.execute(query8)
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
        st.write(df8)

    elif question == "9. average duration of all videos in each channel":
        query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
        cursor.execute(query9)
        t9 = cursor.fetchall()
        df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

        T9 = []
        for index, row in df9.iterrows():
            channel_title = row["channelname"]
            average_duration = row["averageduration"]
            average_duration_str = str(average_duration)
            T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
        df1 = pd.DataFrame(T9)
        st.write(df1)

    elif question == "10. videos with highest number of comments":
        query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos WHERE comments IS
                    NOT NULL ORDER BY comments DESC'''
        cursor.execute(query10)
        t10 = cursor.fetchall()
        df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
        st.write(df10)


def display_tables(cursor):
    st.title("Display Tables")
    tables = ["Channels", "Videos", "Comments", "Playlists"]
    selected_table = st.selectbox("Select Table:", tables)
    if st.button("Display Table"):
        if selected_table == "Channels":
            query = "SELECT * FROM channels"
        elif selected_table == "Videos":
            query = "SELECT * FROM videos"
        elif selected_table == "Comments":
            query = "SELECT * FROM comments"
        elif selected_table == "Playlists":
            query = "SELECT * FROM playlists"
        
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        st.write("Displaying Table:", selected_table)
        st.write(df)


st.title("YouTube Data Harvesting and Warehousing")
tabs = ["Input Channel ID", "Select Questions", "Display Tables"]
selected_tab = st.sidebar.radio("Navigation", tabs)

if selected_tab == "Input Channel ID":
    channel_input()
elif selected_tab == "Select Questions":
    select_questions()
elif selected_tab == "Display Tables":
    display_tables(cursor)


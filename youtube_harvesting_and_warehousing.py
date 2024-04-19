from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime
import re

conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="mohan",
    database="youtube"
)
cursor = conn.cursor()



api='AIzaSyBHQ1IL5tj0gXM_7_zbzROJN8gDfOiOm-Y'
def Api_connect():
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,ContentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        if 'items' in response:
            item = response['items'][0]
            channel_data = {
                'Channel_Name': item["snippet"]["title"],
                'Channel_Id': item["id"],
                'Subscribers': item['statistics']['subscriberCount'],
                'Views': item["statistics"]["viewCount"],
                'Total_Videos': item["statistics"]["videoCount"],
                'Channel_Description': item["snippet"]["description"],
                'Playlist_Id': item["contentDetails"]["relatedPlaylists"]["uploads"]
            }
            return channel_data
        else:
            print("No items found in the response for channel:", channel_id)
            return None
    except Exception as e:
        print("Error occurred while fetching channel info:", str(e))
        return None

def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        try:
            request = youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            )
            response = request.execute()
            if 'items' in response:
                item = response['items'][0]
                data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_Id': item['id'],
                    'Title': item['snippet']['title'],
                    'Tags': item['snippet'].get('tags'),
                    'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'Description': item['snippet'].get('description'),
                    'Published_Date': item['snippet']['publishedAt'],
                    'Duration': item['contentDetails']['duration'],
                    'Views': item['statistics'].get('viewCount'),
                    'Likes': item['statistics'].get('likeCount'),
                    'Comments': item['statistics'].get('commentCount'),
                    'Favorite_Count': item['statistics']['favoriteCount'],
                    'Definition': item['contentDetails']['definition'],
                    'Caption_Status': item['contentDetails']['caption']
                }
                video_data.append(data)
            else:
                print("No items found in the response for video:", video_id)
        except Exception as e:
            print("Error occurred while fetching video info for video:", video_id, str(e))
    return video_data


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


import re
from datetime import datetime

def insert_playlist_details(conn, playlist_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    for playlist_item in playlist_data:
        try:
            published_at = datetime.strptime(playlist_item['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(insert_query, (
                playlist_item['Playlist_Id'],
                playlist_item['Title'],
                playlist_item['Channel_Id'],
                playlist_item['Channel_Name'],
                published_at,  
                playlist_item['Video_Count']
            ))
        except Exception as e:
            print(f"Error inserting playlist data: {e}")
            continue
    conn.commit()
    cursor.close()


from datetime import datetime
import re

def duration_to_seconds(duration_str):
    match = re.match(r'PT(\d+)M(\d+)S', duration_str)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        return minutes * 60 + seconds
    return 0

from datetime import datetime

from datetime import datetime

def insert_video_details(conn, video_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favorite_Count, Definition, Caption_Status)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    for video_item in video_data:
        try:
            published_date = datetime.strptime(video_item['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            duration_seconds = duration_to_seconds(video_item['Duration'])
            tags = ','.join(video_item.get('Tags', [])) if 'Tags' in video_item else ''  # Corrected condition
            cursor.execute(insert_query, (
                video_item['Channel_Name'],
                video_item['Channel_Id'],
                video_item['Video_Id'],
                video_item['Title'],
                tags, 
                video_item['Thumbnail'],
                video_item['Description'],
                published_date,
                duration_seconds,
                int(video_item['Views']),
                int(video_item['Likes']),
                int(video_item['Comments']),
                int(video_item['Favorite_Count']),
                video_item['Definition'],
                video_item['Caption_Status']
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting video details: {e}")
    cursor.close()






def insert_comment_details(conn, comment_data):
    cursor = conn.cursor()
    insert_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                      VALUES (%s, %s, %s, %s, %s)'''
    for comment_item in comment_data:
        try:
            comment_published = datetime.strptime(comment_item['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            print(f"Error parsing date for Comment_Id {comment_item['Comment_Id']}: {e}")
            continue  

        try:
            cursor.execute(insert_query, (
                comment_item['Comment_Id'],
                comment_item['Video_Id'],
                comment_item['Comment_Text'],
                comment_item['Comment_Author'],
                comment_published 
            ))
        except Exception as e:
            print(f"Error inserting comment data: {e}")
            continue
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


def select_questions(cursor):
    question_options = [
        "All the videos and the channel name",
        "Channels with most number of videos",
        "10 most viewed videos",
        "Comments in each video",
        "Videos with highest likes",
        "Likes of all videos",
        "Views of each channel",
        "Videos published in the year of 2022",
        "Average duration of all videos in each channel",
        "Videos with highest number of comments"
    ]
    question = st.selectbox("Select your question", question_options)
    
    if st.button("Display Results"):
        if question == "All the videos and the channel name":
            query = '''SELECT title AS video_title, channel_name AS channel_name FROM videos'''
            column_names = ["Video Title", "Channel Name"]
        elif question == "Channels with most number of videos":
            query = '''SELECT channel_name AS channel_name, total_videos AS total_videos FROM channels 
                        ORDER BY total_videos DESC'''
            column_names = ["Channel Name", "Total Videos"]
        elif question == "10 most viewed videos":
            query = '''SELECT views AS views, channel_name AS channel_name, title AS video_title FROM videos 
                        WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
            column_names = ["Views", "Channel Name", "Video Title"]
        elif question == "Comments in each video":
            query = '''SELECT comments AS no_of_comments, title AS video_title FROM videos WHERE comments IS NOT NULL'''
            column_names = ["No of Comments", "Video Title"]
        elif question == "Videos with highest likes":
            query = '''SELECT title AS video_title, channel_name AS channel_name, likes AS like_count
                        FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
            column_names = ["Video Title", "Channel Name", "Like Count"]
        elif question == "Likes of all videos":
            query = '''SELECT likes AS like_count, title AS video_title FROM videos'''
            column_names = ["Like Count", "Video Title"]
        elif question == "Views of each channel":
            query = '''SELECT channel_name AS channel_name, views AS total_views FROM channels'''
            column_names = ["Channel Name", "Total Views"]
        elif question == "Videos published in the year of 2022":
            query = '''SELECT title AS video_title, published_date AS video_release, channel_name AS channel_name FROM videos
                        WHERE YEAR(published_date) = 2022'''
            column_names = ["Video Title", "Published Date", "Channel Name"]
        elif question == "Average duration of all videos in each channel":
            query = '''SELECT channel_name AS channel_name, AVG(duration) AS average_duration FROM videos GROUP BY channel_name'''
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = ["Channel Name", "Average Duration"]
            df = pd.DataFrame(results, columns=["Channel Name", "Average Duration"])
            df.index += 1
            st.write(df)
            return
        elif question == "Videos with highest number of comments":
            query = '''SELECT title AS video_title, channel_name AS channel_name, comments AS comments FROM videos 
                        WHERE comments IS NOT NULL ORDER BY comments DESC'''
            column_names = ["Video Title", "Channel Name", "Comments"]
        
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=column_names)
        df.index += 1  
        st.write(df)


def display_tables(cursor):
    st.title("Display Tables")
    tables = ["Channels", "Videos", "Comments", "Playlists"]
    selected_table = st.selectbox("Select Table:", tables)
    if st.button("Display Table"):
        if selected_table == "Channels":
            query = "SELECT * FROM channels"
            column_names = ['Channel Name', 'Channel ID', 'Subscribers', 'Views', 'Total Videos', 'Channel Description', 'Playlist ID']
        elif selected_table == "Videos":
            query = "SELECT * FROM videos"
            column_names = ['Channel Name', 'Channel ID', 'Video ID', 'Title', 'Tags', 'Thumbnail', 'Description', 'Published Date', 'Duration', 'Views', 'Likes', 'Comments', 'Favorite Count', 'Definition', 'Caption Status']
        elif selected_table == "Comments":
            query = "SELECT * FROM comments"
            column_names = ['Comment ID', 'Video ID', 'Comment Text', 'Comment Author', 'Comment Published']
        elif selected_table == "Playlists":
            query = "SELECT * FROM playlists"
            column_names = ['Playlist ID', 'Title', 'Channel ID', 'Channel Name', 'Published At', 'Video Count']
        
        cursor.execute(query)
        data = cursor.fetchall()
        if len(data) > 0:
            df = pd.DataFrame(data, columns=column_names)
            df.index += 1  
            st.write("Displaying Table:", selected_table)
            st.write(df)
        else:
            st.write("No data found for selected table.")



st.title("YouTube Data Harvesting and Warehousing")
tabs = ["Input Channel ID", "Select Questions", "Display Tables"]
selected_tab = st.sidebar.radio("Navigation", tabs)

if selected_tab == "Input Channel ID":
    channel_input()
elif selected_tab == "Select Questions":
    select_questions(cursor)
elif selected_tab == "Display Tables":
    display_tables(cursor)


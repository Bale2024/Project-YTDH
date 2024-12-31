import googleapiclient.discovery
import pandas as pd
import re 
import streamlit as st
import datetime as dt
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCgQBu_WIb8gfM-RUyJCRx3LO_oV3ukfNQ"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
# Method to calculate duration of video 
def time2sec(t):
  regex = re.compile(r'P(?:\d+Y)?(?:\d+M)?(?:\d+D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
  match = regex.match(t)
  if not match:
    return None
  hours,minutes,seconds = match.groups()  
  total_sec = (int(hours or 0) * 3600 + int(minutes or 0) * 60 + int(seconds or 0))
  return total_sec
# Method to scrap channel-details from youtube
def channel_data(c_id):
  request = youtube.channels().list(
  part="snippet,contentDetails,statistics",    
  id=c_id 
  )
  response = request.execute()
  data = {"channel_name":response['items'][0]['snippet']['title'],
          "channel_des":response['items'][0]['snippet']['description'],
          "channel_pat":response['items'][0]['snippet']['publishedAt'],
          "channel_plid":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
          "channel_sub":response['items'][0]['statistics']['subscriberCount'],
          "channel_vc":response['items'][0]['statistics']['viewCount'],
          "channel_id":response['items'][0]["id"]
  }
  return data
#data_cg = channel_data("UCtuVvg2mTl1_BlSFqFAHOpw")
# Method to extract playlist ids of a channel
def channel_playlist_ids(c_id):
  playlist_ids=[]
  request = youtube.channels().list(
      part = "snippet,contentDetails",
      id = c_id
    )
  response = request.execute()
  data = {"channel_id":response['items'][0]["id"],
          "playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }   
  return data
# An add_on method 
def channel_fields(c_id):
    primary = []
    request = youtube.channels().list(
    part = "snippet,contentDetails",
    id = c_id
    )
    response = request.execute()
    plid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        video_response = youtube.playlistItems().list(
        part ='snippet',
        playlistId = plid,
        maxResults = 50,
        pageToken = next_page_token
        ).execute()
        for item in video_response['items']:
           data = {"channel_id": item['snippet']['channelId'],
                   "playlist_id":item['snippet']['playlistId'],
                   "video_id":item['snippet']['resourceId']['videoId']
                  }
        
           primary.append(data)  
        next_page_token = video_response.get('nextPageToken')
        if next_page_token is None:
           break
 
    return primary
# Method to extract video_ids of a channel
def channel_videos_ids(c_id):
  video_ids=[]
  request = youtube.channels().list(
      id = c_id,
      part ='contentDetails'
  )
  response = request.execute()
  plid = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token = None
  while True:
    video_response = youtube.playlistItems().list(
      part ='snippet',
      playlistId = plid,
      maxResults=50,
      pageToken=next_page_token
    ).execute()
    for i in range(len(video_response['items'])):
      video_ids.append(video_response['items'][i]['snippet']['resourceId']['videoId']
      )
    next_page_token = video_response.get('nextPageToken')
    if next_page_token is None:
      break
  return video_ids
# Method to exrtact video details of a unique video
def video_details(v_list):
  video_data=[]
  for video_id in v_list:
    request = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id = video_id,
        maxResults=50
    )
    response = request.execute()
    for item in response['items']:
      data ={"video_id":item['id'],
            "video_name":item['snippet']['title'],
            "video_desc":item['snippet']['description'],
            "video_pubat":item['snippet']['publishedAt'],
            "video_vc":item['statistics'].get('viewCount',0),
            "video_likes": item['statistics'].get('likeCount',0),
            "video_favc": item['statistics'].get('favoriteCount',0),
            "video_cmntc": item['statistics'].get('commentCount',0),
            "video_duration": time2sec(item['contentDetails']['duration']),
            "video_captionsts": item['contentDetails']['caption']
      }
      video_data.append(data)
  return video_data
# Method to extract comment details for videos
def comment_details(v_list):
  comments=[]
  
  for video_ids in v_list:
    try:

      request=youtube.commentThreads().list(
          part='snippet',
          videoId=video_ids,
          maxResults=50,
      )
      response = request.execute()
      for i in response['items']:
        data = {"comment_id":i['snippet']['topLevelComment']['id'],
                "video_id":i['snippet']['topLevelComment']['snippet']['videoId'],
                "comment_text":i['snippet']['topLevelComment']['snippet']['textDisplay'],
                "comment_author":i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                "comment_pubdate":i['snippet']['topLevelComment']['snippet']['publishedAt']
        }
        comments.append(data)
    except:
        pass
  return comments
# Establishing Connection with parameters:       
import psycopg2
conn = psycopg2.connect(database="postgres",
    user = "postgres",
    host = "localhost",
    port = "5433",
    password= "7799")
# Creating a cursor object
cur = conn.cursor() 
cur.execute('''CREATE TABLE IF NOT EXISTS channel_details(
            channel_name VARCHAR(255),
            channel_des TEXT,
            channel_pat DATETIME,
            channel_plid VARCHAR(255),
            channel_sub BIGINT,
            channel_vc INTEGER,
            channel_id VARCHAR(255) PRIMARY KEY);
            ''')
conn.commit()
# Method to insert channel details into respective table of SQL database
p_conn = conn
def ins_into_yc(p_conn,p_c_id):
  data_c= channel_data(p_c_id)
  cur.execute( """INSERT INTO channel_details(channel_name,channel_des,channel_pat,channel_plid,
    channel_sub,channel_vc,channel_id)
    VALUES(%s,%s,%s,%s,%s,%s,%s);""",
    (data_c['channel_name'],data_c['channel_des'],data_c['channel_pat'],
    data_c['channel_plid'],data_c['channel_sub'],data_c['channel_vc'],data_c['channel_id']))
  p_conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS playlist_table(
            playlist_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(255) REFERENCES channel_details(channel_id),
            playlist_name VARCHAR(255));
            ''')
# Method to insert playlist ids of channel
def ins_into_pt(p_conn,p_c_id):
    data_p = channel_playlist_ids(p_c_id)
    cur.execute("""INSERT INTO playlist_table(playlist_id,channel_id)
    VALUES(%s,%s);""",
    (data_p['playlist_id'],data_p['channel_id']))
    p_conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS primary_table(
            channel_id VARCHAR(255),
            playlist_id VARCHAR(255),
            video_id VARCHAR(255));  
            ''')
conn.commit()
# Method to insert details of primary table
def ins_into_py(p_conn,p_c_id):
    query ='''INSERT INTO primary_table(channel_id,playlist_id,video_id)
    VALUES(%s,%s,%s)'''
    data_py = channel_fields(p_c_id)
    for i in data_py:
        cur.execute(query,(
            i['channel_id'],i['playlist_id'],i['video_id']
        ))
    p_conn.commit()

cur.execute('''CREATE TABLE IF NOT EXISTS video_table(
            video_id VARCHAR(255) PRIMARY KEY,
            Plid VARCHAR(255) REFERENCES playlist_table(playlist_id),
            video_name VARCHAR(255),
            video_desc VARCHAR(255),
            video_pubat DATETIME,
            video_vc BIGINT,
            video_likes INT,
            video_favc INT,
            video_cmntc INT,
            video_duration INT,
            video_captionsts VARCHAR(255));
            ''')
conn.commit()
# Method to insert video details into video_table
def ins_into_vdt(p_conn,p_c_id):
    query = '''INSERT INTO video_table(video_id, video_name,video_desc,video_pubat,video_vc,video_likes, 
    video_favc,video_cmntc,video_duration,video_captionsts)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    data_vds = channel_videos_ids(p_c_id)
    data_vt = video_details(data_vds)
    st.write(data_vt)
    for i in data_vt:
      cur.execute(query,(
            i['video_id'],i['video_name'],i['video_desc'],i['video_pubat'],i['video_vc'],
                    i['video_likes'],i['video_favc'],i['video_cmntc'],i['video_duration'],i['video_captionsts']))
      p_conn.commit()

cur.execute("""CREATE TABLE IF NOT EXISTS comment_table(
            comment_id VARCHAR(255),
            video_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_pubdate TIMESTAMP);
            """ )
conn.commit()
# Method to insert comment details of videos into comment table
def ins_into_ct(p_conn,p_c_id):
    query = '''INSERT INTO comment_table(comment_id,video_id,comment_text,comment_author,comment_pubdate)
    VALUES(%s,%s,%s,%s,%s)'''
    data_vds = channel_videos_ids(p_c_id)
    data_cm = comment_details(data_vds)
    for i in data_cm:
        cur.execute(query,(i['comment_id'],i['video_id'],i['comment_text'],
                          i['comment_author'],i['comment_pubdate']))
    p_conn.commit()
# Main function to insert all details of a channel into corresponding tables:
def ins_into_ytchnl(p_conn,p_c_id):
  data_ft = ins_into_yc(p_conn,p_c_id)
  data_st = ins_into_pt(p_conn,p_c_id)
  data_tt = ins_into_py(p_conn,p_c_id)
  data_ft = ins_into_vdt(p_conn,p_c_id)
  data_lt = ins_into_ct(p_conn,p_c_id)
  print("Data Inserted Successfully")
## Query using Streamlit App
st.header("Youtube Data Harvesting")
user_inp = st.text_input('Enter the Channel_id of a Youtube Channel')
if user_inp and st.button("Transfer Channel data to SQL Database"):
  data_chnl = ins_into_ytchnl(conn,user_inp)
  st.balloons()
question = st.selectbox("Select your question",("1.All the videos and channel names",
                                                "2.Channels having most videos, no.of videos",
                                                "3.top 10 most viewed videos",
                                                "4.Comments made for each video",
                                                "5.Video with highest no. of likes",
                                                "6.likes and dislikes for each video",
                                                "7.Total no.of views for each channel",
                                                "8.Channels that have published videos in 2022",
                                                "9.Average duration of all videos",
                                                "10.Videos having highest no.of comments"))
st.write("Choose a Question to view the Insights about the Youtube Channels Data stored in DB")  
if question=="1.All the videos and channel names":
  st.write("Displaying all the videos and channel names.")
  query1 = ('''select channel_name,video_name
            from channel_details as CD
            join playlist_table as PT on PT.channel_id = CD.channel_id
            join primary_table as PY on PY.playlist_id = PT.playlist_id
            join video_table as VT on VT.video_id = PY.video_id;''')
  df = pd.read_sql(query1,conn)
  st.dataframe(df)

elif question=="2.Channels having most videos, no.of videos":
   st.write("Displaying channel with most no of videos alongside count")  
   query2 =('''select CD.channel_name, count(VT.video_id) as vc
            from channel_details as CD
            join playlist_table as PT on PT.channel_id = CD.channel_id
            join primary_table as PY on PY.playlist_id = PT.playlist_id
            join video_table as VT on VT.video_id = PY.video_id
            group by CD.channel_name
            order by vc desc
            limit 1;''')
   df2 = pd.read_sql(query2,conn)
   st.dataframe(df2)
elif question=="3.top 10 most viewed videos":
   st.write("Displaying videos with maximum view count")
   query3 =('''select CD.channel_name,VT.video_name
            from channel_details as CD
            join playlist_table as PT on PT.channel_id = CD.channel_id
            join primary_table as PY on PY.playlist_id = PT.playlist_id
            join video_table as VT on VT.video_id = PY.video_id
            group by CD.channel_name,VT.video_name, VT.video_vc 
            limit 10;''')
   df3 = pd.read_sql(query3,conn)
   st.dataframe(df3)
elif question=="4.Comments made for each video":  
   st.write("Displaying Comments of Each video")
   query4 = ('''select VT.video_name, VT.video_cmntc
              from channel_details as CD
              join playlist_table as PT on PT.channel_id = CD.channel_id
              join primary_table as PY on PY.playlist_id = PT.playlist_id
              join video_table as VT on VT.video_id = PY.video_id
              order by video_cmntc desc;
              ''')
   df4 = pd.read_sql(query4,conn)
   st.dataframe(df4)
elif question=="5.Video with highest no. of likes":
   st.write("Displaying video with max count of likes")
   query5 = ('''select video_name,video_likes
            from video_table
            order by video_likes desc
            limit 1;''')
   df5= pd.read_sql(query5,conn)
   st.dataframe(df5)
elif question=="6.likes and dislikes for each video":
   st.write("Displaying like count for each video ")
   query6 = ('''select video_name,video_likes
            from video_table
            order by video_likes;''')
   df6 = pd.read_sql(query6,conn)
   st.dataframe(df6)
elif question=="7.Total no.of views for each channel":
   st.write("Displaying total view count for all Channels")
   query7 = ('''select channel_name,channel_vc as tvc
            from channel_details;''')
   df7 = pd.read_sql(query7,conn)
   st.dataframe(df7)
elif question=="8.Channels that have published videos in 2022":
    st.write("Displaying Channel that have published videos in year 2022")
    query8  =('''select channel_name, video_name,video_pubat
              from channel_details as CD
              join playlist_table as PT on PT.channel_id = CD.channel_id
              join primary_table as PY on PY.playlist_id = PT.playlist_id
              join video_table as VT on VT.video_id = PY.video_id
              where extract(year from video_pubat)= 2022
              order by channel_name;''')
    df8 = pd.read_sql(query8,conn)
    st.dataframe(df8)
elif question=="9.Average duration of all videos":
   st.write("Displaying average duration of all the videos")
   query9 = ('''select CD.channel_name, AVG(video_duration)as AVD
            from channel_details as CD
            join playlist_table as PT on PT.channel_id = CD.channel_id
            join primary_table as PY on PY.playlist_id = PT.playlist_id
            join video_table as VT on VT.video_id = PY.video_id
            group by channel_name
            order by AVD;	''')
   df9 = pd.read_sql(query9,conn)
   st.dataframe(df9)
elif question=="10.Videos having highest no.of comments":
  st.write("Displaying the Videos with max.no of Comments")
  query10 = ('''select CD.channel_name, VT.video_name, VT.video_cmntc
          from channel_details as CD
          join playlist_table as PT on PT.channel_id = CD.channel_id
          join primary_table as PY on PY.playlist_id = PT.playlist_id
          join video_table as VT on VT.video_id = PY.video_id
          order by video_cmntc desc
          limit 10;	''')
  df10 = pd.read_sql(query10,conn)
  st.dataframe(df10)
      
          


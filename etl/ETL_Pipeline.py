'''
PART I: IMPORTING LIBRARIES
'''

# libraries for YouTube API
import googleapiclient.discovery
from googleapiclient.discovery import build

# libraries for Reddit API
import praw

# libraries for MongoDB
from pymongo import MongoClient

# libraries for Airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# general libraries
import datetime
from datetime import timedelta

import isodate
import re
import emoji
import os
import logging
import sys
import pandas as pd
import numpy as np

# libraries for nltk
import nltk
import spacy
from nltk.corpus import stopwords 
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

nltk.download('vader_lexicon')
nltk.download('stopwords')

nlp = spacy.load("en_core_web_sm")

'''
PART II: AIRFLOW CONFIGURATION
'''

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# define the default arguments
default_args = {
    'owner': 'HuyNgo',
    'email': 'ngohuy171099@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'etl_pipeline_iphone16_sentiment_analysis',
    default_args=default_args,
    description='An ETL pipeline that extracts headlines from subreddits and comments from YouTube videos related to the iPhone 16, \
        cleans and transforms the data, and finally, loads it into a MongoDB database',
    schedule_interval=timedelta(days=7),  
    start_date=days_ago(0),               
    catchup=False,                        
    is_paused_upon_creation=False       
)

# create a directory called 'app/files' inside the container
data_dir = '/usr/local/airflow/files'
os.makedirs(data_dir, exist_ok=True)

'''
PART III: YOUTUBE API AND REDDIT API
'''

# Reddit API
user_agent = 'iPhone 16 Sentiment Analysis by /u/Apprehensive-Car2349'
reddit_client_id = os.getenv('REDDIT_CLIENT_ID')
reddit_client_secret = os.getenv('REDDIT_CLIENT_SECRET')

if not reddit_client_id or not reddit_client_secret:
    logger.error("Reddit API credentials are missing.")
    sys.exit("Please set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables.")

reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=user_agent
)

# YouTube API
api_key = os.getenv('YOUTUBE_API_KEY')
if not api_key:
    logger.error("YouTube API key is missing.")
    sys.exit("Please set the YOUTUBE_API_KEY environment variable.")

youtube = build('youtube', 'v3', developerKey=api_key)

# log a message to confirm that the YouTube API and Reddit API clients have been successfully initialized
logger.info("YouTube API and Reddit API clients successfully initialized.")

'''
PART IV: EXTRACT FUNCTIONS
'''

# define a function to get headlines from relevant subreddits
def extract_Reddit():
    headlines = set()
    subreddits = ['apple', 'iphone', 'technology']
    
    for subreddit in subreddits:
        # get all the latest headlines from each subreddit
        for submission in reddit.subreddit(subreddit).new(limit=None):
            headlines.add(submission.title)
            
    reddit_headlines_df = pd.DataFrame(list(headlines), columns=['headline'])
    
    reddit_headlines_csv_path = os.path.join(data_dir, 'reddit_headlines_df.csv')
    reddit_headlines_df.to_csv(reddit_headlines_csv_path, index=False)
    logger.info(f"Reddit headlines saved to {reddit_headlines_csv_path}")


# define a function to get the top 50 videos posted in the last month with the highest view count for the search term "iphone 16"
def get_top_videos(query, published_after, youtube, top_n=50, max_results=50):
    videos = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            publishedAfter=published_after,
            order="date",
            pageToken=next_page_token
        )
        
        response = request.execute()

        # retrieve video IDs from the search results
        video_ids = [video['id']['videoId'] for video in response.get('items', [])]

        # Fetch video details including statistics, content details, and comments enabled/disabled status
        if video_ids:
            video_details_request = youtube.videos().list(
                # use contentDetails to get the duration of the video
                part="snippet,statistics,contentDetails",  
                id=",".join(video_ids),
                maxResults=max_results
            )
            
            video_details_response = video_details_request.execute()

            for video in video_details_response['items']:
                # parse duration from the contentDetails field
                duration = video['contentDetails']['duration']
                # convert ISO 8601 duration format to total seconds
                total_seconds = isodate.parse_duration(duration).total_seconds()
                # only include videos longer than 60 seconds to filter out Shorts
                if total_seconds > 60:
                    view_count = int(video['statistics'].get('viewCount', 0))
                    comment_count_str = video['statistics'].get('commentCount', '0')
                    comment_count = int(comment_count_str) if comment_count_str.isdigit() else 0
                    video_data = {
                        'id': video['id'],
                        'title': video['snippet']['title'],
                        'view_count': view_count,
                        'comment_count': comment_count,
                        'comments_enabled': comment_count > 0,
                        'duration': total_seconds 
                    }
                    # only include videos where comments are enabled
                    if video_data['comments_enabled']:
                        videos.append(video_data)

        # check if there is a next page token
        next_page_token = response.get('nextPageToken')

        # if there is no next page token, all videos have been retrieved, so break the loop
        if not next_page_token:
            break

    # sort videos by view count in descending order
    sorted_videos = sorted(videos, key=lambda x: x['view_count'], reverse=True)

    # return the top 50 videos
    return sorted_videos[:top_n]


# define a function to extract the top 200 comments with the highest like count 
def get_top_comments(video_id, youtube, top_n=200, max_results=100):
    comments = []
    next_page_token = None

    # loop through pages of comments until there are no more pages
    while True:
        # fetch a page of comments
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_results,
            pageToken=next_page_token,
            textFormat="plainText"
        )
        
        response = request.execute()

        # append the comments and their like counts to the list
        for item in response['items']:
            comment_data = item['snippet']['topLevelComment']['snippet']
            comment = {
                'text': comment_data['textDisplay'],
                'likes': int(comment_data.get('likeCount', 0)),
                'author': comment_data['authorDisplayName'],
            }
            comments.append(comment)

        # check if there is a next page token (i.e., more comments)
        next_page_token = response.get('nextPageToken')

        # if there is no nextPageToken, all comments have been retrieved, so break the loop
        if not next_page_token:
            break

    # sort the comments by the number of likes in descending order
    sorted_comments = sorted(comments, key=lambda x: x['likes'], reverse=True)

    # return the top 200 comments
    return sorted_comments[:top_n]


# define a function to extract the top 200 comments from the top 50 videos
def get_top_comments_from_top_videos(videos, youtube, top_n_comments=200):
    all_top_comments = {}

    # loop through the top 50 videos and extract the top 200 comments from each
    for idx, video in enumerate(videos):
        video_id = video['id'] 
        video_title = video['title']
        # fetch top comments for the video
        top_comments = get_top_comments(video_id, youtube, top_n=top_n_comments)
        # store the video title and top comments in the result dictionary
        all_top_comments[video_id] = {
            'video_title': video_title,
            'top_comments': top_comments
        }
    
    return all_top_comments


# define a function to flatten comments data 
def flatten_comments_data(top_comments_data):
    rows = []
    
    for video_id, video_info in top_comments_data.items():
        video_title = video_info['video_title']
        top_comments = video_info['top_comments']
        # each comment will become a row with the video title, video ID, and comment text
        for comment in top_comments:
            rows.append({
                'video_id': video_id,
                'video_title': video_title,
                'comment': comment['text']
            })
    
    return rows


# define a function to get comments from YouTube
def extract_YouTube():
    one_month_period = (datetime.datetime.now() - datetime.timedelta(days=30)).isoformat() + 'Z'
    top_50_videos = get_top_videos('iphone 16', one_month_period, youtube)
    top_200_comments_per_top_50_videos = get_top_comments_from_top_videos(top_50_videos, youtube)
    top_200_comments_per_top_50_videos_flattened = flatten_comments_data(top_200_comments_per_top_50_videos)

    youtube_comments_df = pd.DataFrame(top_200_comments_per_top_50_videos_flattened, columns=['video_id', 'video_title', 'comment'])
    youtube_comments_csv_path = os.path.join(data_dir, 'youtube_comments_df.csv')
    youtube_comments_df.to_csv(youtube_comments_csv_path, index=False)
    logger.info(f"YouTube comments saved to {youtube_comments_csv_path}")

'''
PART V: CLEAN FUNCTIONS
'''

stop_words = set(stopwords.words('english')) | {
        'i', 'me', 'my', 'mine', 'we', 'us', 'our', 'you', 'your', 
        'he', 'him', 'his', 'she', 'her', 'it', 'its', 
        'is', 'are', 'am', 'do', 'does', 'did', 'be', 'been', 'being', 
        'have', 'has', 'had', 'can', 'could', 'will', 'would', 'shall', 'should', 'may', 'might', 'must',
        'myself', 'ourselves', 'yourself', 'yourselves', 'himself', 'herself', 'themselves',
        'what', 'which', 'who', 'whom', 'whose', 'where', 'when', 'why', 'how',
        'they', 'them', 'their', 'that', 'this', 'there', 'here', 'a', 'an', 'the',
        'was', 'were', 'in', 'on', 'of', 'at', 'for', 'with', 'as', 'to', 'from', 'by', 
        'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 
        'below', 'up', 'down', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
        'thank', 'thanks', 'please', 'plz', 'pls', 'yes', 'no', 'yeah', 'yep', 'yup', 'ok', 'okay',
        'subscribe', 'subscribed', 'subscribing', 'like', 'liked', 'liking',
        'bro', 'no one', 'nobody', 'none', 'everybody', 'everyone', 'everything', 'everywhere', 'anyone', 'anybody', 'anything', 'anywhere',
    }

def clean(row):
    # remove emojis and special characters
    row = emoji.replace_emoji(row, replace=' ')
    row = re.sub(r'[^A-Za-z0-9\s]', ' ', row)
    
    # convert to lowercase
    row = row.lower()
    
    # remove stopwords
    row = ' '.join([word for word in row.split() if word not in stop_words])
    
    # remove trailing spaces
    row = row.strip()
    
    return row

def clean_Reddit():
    try:
        # load the reddit_headlines_df from the CSV file
        reddit_headlines_df = pd.read_csv(os.path.join(data_dir, 'reddit_headlines_df.csv'))
        
        # create a new column called 'cleaned_headline' by applying the clean function to the 'headline' column
        reddit_headlines_df['cleaned_headline'] = reddit_headlines_df['headline'].apply(clean)
        
        # drop rows where the cleaned_headline is null or an empty string
        reddit_headlines_df = reddit_headlines_df[reddit_headlines_df['cleaned_headline'].notnull() & 
                                                  reddit_headlines_df['cleaned_headline'].str.strip().astype(bool)]
        
        # select only the 'headline' and 'cleaned_headline' columns
        reddit_headlines_cleaned_df = reddit_headlines_df[['headline', 'cleaned_headline']]
        
        # save the reddit_headlines_cleaned_df to a CSV file
        reddit_headlines_cleaned_csv_path = os.path.join(data_dir, 'reddit_headlines_cleaned_df.csv')
        reddit_headlines_cleaned_df.to_csv(reddit_headlines_cleaned_csv_path, index=False)
        logger.info(f"Cleaned Reddit headlines saved to {reddit_headlines_cleaned_csv_path}")
    
    except Exception as e:
        logger.error("Error in cleaning Reddit headlines: %s", e)
    
def clean_YouTube():
    try:
        # load the YouTube comments DataFrame
        youtube_comments_df = pd.read_csv(os.path.join(data_dir, 'youtube_comments_df.csv'))
        
        # drop rows where 'comment' is not a string (like NaN or other types)
        youtube_comments_df = youtube_comments_df[youtube_comments_df['comment'].apply(lambda x: isinstance(x, str))]
        
        # apply the `clean` function to each comment
        youtube_comments_df['cleaned_comment'] = youtube_comments_df['comment'].apply(clean)
        
        # drop rows where `cleaned_comment` is null or an empty string
        youtube_comments_df = youtube_comments_df[
            youtube_comments_df['cleaned_comment'].notnull() &
            youtube_comments_df['cleaned_comment'].str.strip().astype(bool)
        ]
        
        # select only the `comment` and `cleaned_comment` columns
        youtube_comments_cleaned_df = youtube_comments_df[['comment', 'cleaned_comment']]
        
        # save the cleaned DataFrame to a CSV file
        youtube_comments_cleaned_csv_path = os.path.join(data_dir, 'youtube_comments_cleaned_df.csv')
        youtube_comments_cleaned_df.to_csv(youtube_comments_cleaned_csv_path, index=False)
        logger.info(f"Cleaned YouTube comments saved to {youtube_comments_cleaned_csv_path}")
        
    except Exception as e:
        logger.error("Error in cleaning YouTube comments: %s", e)


'''
PART VI: TRANSFORM FUNCTIONS
'''

def add_columns(row):
    try:
        phone_models = ['samsung', 'huawei', 'xiaomi', 'nokia']
        topics_dict = {
            'dimensions': {'size', 'dimensions', 'length', 'width', 'height', 'compact'},
            'OS': {'operating system', 'iOS', 'Android', 'software', 'update', 'version'},
            'screen size': {'screen size', 'display', 'inches', 'size', 'screen'},
            'resolution': {'resolution', 'pixel', 'PPI', 'HD', '4K', '2K', 'FHD', 'retina'},
            'CPU': {'CPU', 'processor', 'chip', 'performance', 'speed', 'cores', 'GHz'},
            'RAM': {'RAM', 'memory', 'gigabytes', 'GB', 'performance', 'multitasking'},
            'storage': {'storage', 'internal storage', 'capacity', 'GB', 'TB', 'memory'},
            'battery': {'battery', 'mAh', 'battery life', 'charge', 'charging', 'power'},
            'camera': {'camera', 'megapixels', 'photo', 'picture', 'video', 'selfie', 'lens', 'image'},
            'performance': {'performance', 'fast', 'speed', 'lag', 'smooth', 'gaming', 'multi-tasking'},
            'design': {'design', 'style', 'look', 'color', 'build quality', 'material', 'aesthetic'},
            'price': {'price', 'cost', 'expensive', 'cheap', 'affordable', 'value'},
        }
    
        found_phone_models = set()
        found_topics = set()
        
        # check for phone models in each row
        for model in phone_models:
            if model in row:
                found_phone_models.add(model)
                
        # check for topics in each row
        for topic, keywords in topics_dict.items():
            if any(keyword in row for keyword in keywords):
                found_topics.add(topic)
                
        return (list(found_phone_models), list(found_topics))
    
    except Exception as e:
        logger.error("Error in adding columns: %s", e)
        return ([], [])
         
def transform_YouTube():
    try:
        # load the youtube_comments_cleaned_df from the CSV file
        youtube_comments_cleaned_df = pd.read_csv(os.path.join(data_dir, 'youtube_comments_cleaned_df.csv'))
    
        # apply the add_columns_YouTube function to the 'cleaned_comment' column
        youtube_comments_cleaned_df['phone_models'], youtube_comments_cleaned_df['topics'] = zip(*youtube_comments_cleaned_df['cleaned_comment'].map(add_columns))
        
        # save the youtube_comments_transformed_df to a CSV file
        youtube_comments_transformed_json_path = os.path.join(data_dir, 'youtube_comments_transformed_df.json')
        youtube_comments_cleaned_df.to_json(youtube_comments_transformed_json_path, orient='records', lines=True)
        logger.info(f"YouTube comments transformed and saved to {youtube_comments_transformed_json_path}")  
            
    except Exception as e:
        logger.error("Error in transforming YouTube comments: %s", e)
        
def transform_Reddit():
    try:
        # load the reddit_headlines_cleaned_df from the CSV file
        reddit_headlines_cleaned_df = pd.read_csv(os.path.join(data_dir, 'reddit_headlines_cleaned_df.csv'))
        
        # filter rows to include only those with "iphone" in 'cleaned_headline'
        reddit_headlines_cleaned_df = reddit_headlines_cleaned_df[reddit_headlines_cleaned_df['cleaned_headline'].str.contains('iphone', na=False)]

        # apply the add_columns_Reddit function to the 'cleaned_headline' column
        reddit_headlines_cleaned_df['phone_models'], reddit_headlines_cleaned_df['topics'] = zip(*reddit_headlines_cleaned_df['cleaned_headline'].map(add_columns))
        
        # save the reddit_headlines_transformed_df to a CSV file
        reddit_headlines_transformed_json_path = os.path.join(data_dir, 'reddit_headlines_transformed_df.json')
        reddit_headlines_cleaned_df.to_json(reddit_headlines_transformed_json_path, orient='records', lines=True)
        logger.info(f"Reddit headlines transformed and saved to {reddit_headlines_transformed_json_path}")
    
    except Exception as e:
        logger.error("Error in transforming Reddit headlines: %s", e)

'''
PART VII: SENTIMENT ANALYSIS
'''

sia = SIA()

def analyze_sentiment(row):
    try:
        sentiment_scores = sia.polarity_scores(row)
        if sentiment_scores['compound'] > 0.2:
            return 'Positive'
        elif sentiment_scores['compound'] < -0.2:
            return 'Negative'
        else:
            return 'Neutral'
    except Exception as e:
        logger.error("Error in sentiment analysis: %s", e)
        return None

def analyze_sentiment_YouTube():
    try:
        # load the youtube_comments_transformed_df from the CSV file
        youtube_comments_transformed_df = pd.read_json(os.path.join(data_dir, 'youtube_comments_transformed_df.json'), orient='records', lines=True)
        
        # create a new column called 'sentiment' by applying the analyze_sentiment function to the 'cleaned_comment' column
        youtube_comments_transformed_df['sentiment'] = youtube_comments_transformed_df['cleaned_comment'].apply(analyze_sentiment)
        
        # save the youtube_comments_sentiment_df to a CSV file
        youtube_comments_sentiment_json_path = os.path.join(data_dir, 'youtube_comments_sentiment_df.json')
        youtube_comments_transformed_df.to_json(youtube_comments_sentiment_json_path, orient='records', lines=True)
        logger.info(f"YouTube comments sentiment analysis saved to {youtube_comments_sentiment_json_path}")

    except Exception as e:
        logger.error("Error in analyzing sentiment for YouTube comments: %s", e)

def analyze_sentiment_Reddit():
    try:
        # load the reddit_headlines_transformed_df from the CSV file
        reddit_headlines_transformed_df = pd.read_json(os.path.join(data_dir, 'reddit_headlines_transformed_df.json'), orient='records', lines=True)
        
        # create a new column called 'sentiment' by applying the analyze_sentiment function to the 'cleaned_headline' column
        reddit_headlines_transformed_df['sentiment'] = reddit_headlines_transformed_df['cleaned_headline'].apply(analyze_sentiment)
        
        # save the reddit_headlines_sentiment_df to a CSV file
        reddit_headlines_sentiment_json_path = os.path.join(data_dir, 'reddit_headlines_sentiment_df.json')
        reddit_headlines_transformed_df.to_json(reddit_headlines_sentiment_json_path, orient='records', lines=True)
        logger.info(f"Reddit headlines sentiment analysis saved to {reddit_headlines_sentiment_json_path}")

    except Exception as e:
        logger.error("Error in analyzing sentiment for Reddit headlines: %s", e)

'''
PART VIII: LOAD FUNCTIONS
'''

# define a function to load the data into MongoDB 
def load():
    try:
        # establish the connection to MongoDB
        client = MongoClient('mongodb', 27017, username=os.getenv('ROOT_USERNAME'), password=os.getenv('ROOT_PASSWORD'))
    
        # create a database called 'iphone16_sentiment_analysis'
        db = client.iphone16_sentiment_analysis
    
        # create a collection called 'reddit_headlines'
        reddit_headlines_collection = db.reddit_headlines
    
        # create a collection called 'youtube_comments'
        youtube_comments_collection = db.youtube_comments
    
        youtube_comments_sentiment_df = pd.read_json(os.path.join(data_dir, 'youtube_comments_sentiment_df.json'), orient='records', lines=True)
        reddit_headlines_sentiment_df = pd.read_json(os.path.join(data_dir, 'reddit_headlines_sentiment_df.json'), orient='records', lines=True)
    
        # convert the Spark dataframes to lists of dictionaries
        youtube_as_dicts = youtube_comments_sentiment_df.to_dict(orient='records')
        reddit_as_dicts = reddit_headlines_sentiment_df.to_dict(orient='records')
    
        # insert the data into the MongoDB collections
        youtube_comments_collection.insert_many(youtube_as_dicts)
        reddit_headlines_collection.insert_many(reddit_as_dicts)
        logger.info("Data successfully loaded into MongoDB.")
        
    except Exception as e:
        logger.error("Error in loading data to MongoDB: %s", e)
        
    finally:
        client.close()
        logger.info("MongoDB connection closed.")
    
'''
PART IX: DEFINE TASKS
'''

extract_Reddit_task = PythonOperator(
    task_id='extract_Reddit',
    python_callable=extract_Reddit,
    dag=dag,
)

extract_YouTube_task = PythonOperator(
    task_id='extract_YouTube',
    python_callable=extract_YouTube,
    dag=dag,
)

clean_Reddit_task = PythonOperator(
    task_id='clean_Reddit',
    python_callable=clean_Reddit,
    dag=dag,
)

clean_YouTube_task = PythonOperator(
    task_id='clean_YouTube',
    python_callable=clean_YouTube,
    dag=dag,
)

transform_Reddit_task = PythonOperator(
    task_id='transform_Reddit',
    python_callable=transform_Reddit,
    dag=dag,
)

transform_YouTube_task = PythonOperator(
    task_id='transform_YouTube',
    python_callable=transform_YouTube,
    dag=dag,
)

sentiment_Reddit_task = PythonOperator(
    task_id='analyze_sentiment_Reddit',
    python_callable=analyze_sentiment_Reddit,
    dag=dag,
)

sentiment_YouTube_task = PythonOperator(
    task_id='analyze_sentiment_YouTube',
    python_callable=analyze_sentiment_YouTube,
    dag=dag,
)

wait_for_load_task = EmptyOperator(
    task_id='wait_for_load',
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

'''
PART X: SET TASK DEPENDENCIES
'''

extract_Reddit_task >> clean_Reddit_task >> transform_Reddit_task >> sentiment_Reddit_task >> wait_for_load_task
extract_YouTube_task >> clean_YouTube_task >> transform_YouTube_task >> sentiment_YouTube_task >> wait_for_load_task
wait_for_load_task >> load_task
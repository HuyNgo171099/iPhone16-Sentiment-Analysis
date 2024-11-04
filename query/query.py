# MongoDB libraries
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# general libraries
import pandas as pd
import numpy as np
import os
import gc

# visualization libraries
import seaborn as sns
import matplotlib.pyplot as plt

# set up output directory
output_dir = '/app/output/'
os.makedirs(output_dir, exist_ok=True)

# establish MongoDB connection
try:
    client = MongoClient('mongodb', 27017, username=os.getenv('ROOT_USERNAME'), password=os.getenv('ROOT_PASSWORD'))
    client.admin.command('ping')
    print("MongoDB connection successful.")
except ConnectionFailure as e:
    print(f"MongoDB connection failed: {e}")
    exit(1)

# connect to the specific database and collections
db = client.iphone16_sentiment_analysis
collection_reddit = db.reddit_headlines
collection_youtube = db.youtube_comments

# define a function to visualize sentiment distribution as a pie chart
def pie_chart(collection, filename, title=None, color_palette="Paired"):
    try:
        # default title if not provided
        if not title:
            title = "Sentiment Distribution in Reddit Headlines" if 'reddit' in collection.name.lower() else "Sentiment Distribution in YouTube Comments"

        # fetch sentiment counts and format data for pie chart
        sentiment_counts = list(collection.aggregate([{"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}]))
        labels = [doc['_id'] for doc in sentiment_counts]
        sizes = [doc['count'] for doc in sentiment_counts]
        
        # generate color palette
        colors = sns.color_palette(color_palette, len(labels))

        # create pie chart
        plt.figure(figsize=(8, 8))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, shadow=True, explode=[0.1]*len(labels))
        plt.axis('equal')
        plt.title(title)
        
        # save and close
        plt.savefig(os.path.join(output_dir, filename), format='png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved pie chart to {filename}")

    except Exception as e:
        print(f"Error creating pie chart for {collection.name}: {e}")

# define a function to visualize sentiment distribution per topic as a heatmap
def heatmap(collection, filename, title=None, color_palette="YlGnBu"):
    try:
        # default title if not provided
        if not title:
            title = "Sentiment Count by Topic in Reddit Headlines" if 'reddit' in collection.name.lower() else "Sentiment Count by Topic in YouTube Comments"
        
        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": {"topics": {"$ne": None}}},
            {"$unwind": "$topics"},
            {"$group": {"_id": {"topic": "$topics", "sentiment": "$sentiment"}, "count": {"$sum": 1}}},
            {"$sort": {"topic": 1, "sentiment": 1}}
        ]
        
        # execute the pipeline and collect results
        result = collection.aggregate(pipeline)
        
        # extract data for heatmap
        data = [{"topic": doc["_id"]["topic"], "sentiment": doc["_id"]["sentiment"], "count": doc["count"]} for doc in result]
        df = pd.DataFrame(data)
        
        # pivot data for heatmap
        pivot_df = df.pivot_table(index="topic", columns="sentiment", values="count", fill_value=0)

        # create heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_df, annot=True, cmap=color_palette, fmt="g")
        plt.title(title)
        plt.xlabel("Sentiment")
        plt.ylabel("Topic")

        # save and close
        plt.savefig(os.path.join(output_dir, filename), format='png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved heatmap to {filename}")

    except Exception as e:
        print(f"Error creating heatmap for {collection.name}: {e}")

    finally:
        gc.collect()

# export Reddit sentiment pie chart to a PNG file
pie_chart(collection_reddit, "reddit_sentiment_pie_chart.png")

# export YouTube sentiment pie chart to a PNG file
pie_chart(collection_youtube, "youtube_sentiment_pie_chart.png")

# export Reddit sentiment heatmap to a PNG file   
heatmap(collection_reddit, "reddit_sentiment_by_topic_heatmap.png")

# export YouTube sentiment heatmap to a PNG file
heatmap(collection_youtube, "youtube_sentiment_by_topic_heatmap.png")

# close MongoDB client
client.close()

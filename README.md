# iPhone 16 Sentiment Analysis

## Project Overview

This project aims to build an ETL pipeline that extracts YouTube comments and Reddit headlines related to the newly launched iPhone 16. The pipeline cleans and transforms the data, conducts sentiment analysis to assess public opinion, loads the processed data into a MongoDB database, and queries the database to generate visualizations for reporting key insights.

## Tools and Languages

- **Tools**: Docker Compose, Apache Airflow, MongoDB (Compass), and REST APIs 
- **Languages**: Python and MongoDB Query Language (MQL)

## Key Tasks

- **YouTube Comments**: Collected the top 200 comments with the highest like count from each of the top 50 videos  posted in the past month with the highest view count for the search term ‘iphone 16’ using the **YouTube API**.
- **Reddit Headlines**: Retrieved the latest Reddit headlines from the subreddits `apple`, `iphone`, and `technology` using the **Reddit API**. 
- **Data Cleaning**: Used **Python** to clean the comments and headlines by removing emojis, special characters, converting text to lowercase, removing stopwords, and trimming spaces.
- **Data Transformation**: Added columns for `phone_models` and `topics`. The `phone_models` column identifies if a comment or headline compares the iPhone 16 with any other phone models (e.g., Samsung Galaxy S24). The `topics` column checks if a comment or headline mentions any specific subjects like screen size, resolution, CPU, RAM, or storage.
- **Sentiment Analysis**: Performed sentiment analysis on each comment and headline. The sentiments were classified as "Positive," "Negative," or "Neutral.".
- **Data Loading**: Stored data in a **MongoDB** database with two collections, `reddit_headlines_collection` and `youtube_comments_collection`. CSV exports of the data were saved for local storage as backups.
- **Data Visualization**: Queried the **MongoDB** database and generated visualizations to summarize key insights.
- **Workflow Management**: Coordinated the entire ETL process using **Apache Airflow** for reliable data extraction, transformation, and loading.
- **Containerization**: Containerized the project with **Docker Compose** for easy deployment in a production environment.

## Running The Project

### Prerequisites

- Docker and Docker Compose installed
- MongoDB Compass installed
- Python 3.x. 

### Installation and Setup

1. **Clone The Repository**

   ```sh
   git clone https://github.com/HuyNgo171099/iPhone16-Sentiment-Analysis-Pipeline
   cd Travel-Agency-Pipeline 

2. **Docker Setup for MongoDB and Airflow**

   ```sh
   docker-compose up --build mongodb etl

3. **Airflow Webserver GUI (localhost:8080)**

4. **Docker Setup for Query**

   ```sh
   docker-compose up --build query

4. **MongoDB Compass Connection (Optional)**

## ETL Pipeline

### Extract Stage

- **Reddit Headlines**: Extracted recent headlines from the subreddits `apple`, `iphone`, and `technology` using the Reddit API. 
- **YouTube Comments**: Identified the top 50 videos posted in the past month with the highest view count for the search term "iPhone 16" (excluding short videos under 60 seconds). Extracted the top 200 comments with the highest like count from each video using the YouTube API.

### Transform Stage

#### Data Cleaning

Created a list of stopwords (common but unimportant words) and defined a `clean` function that:
- Removes emojis and special characters
- Converts text to lowercase
- Removes stopwords and trailing spaces
- Drops empty or null comments, especially those left empty after cleaning.

#### Topic and Model Identification

Developed a function to add `phone_models` and `topics` columns:
- `phone_models`: Flags mentions of competing phone models, such as Samsung, Huawei, and Xiaomi.
- `topics`: Identifies topics related to the iPhone 16 (e.g., screen size, battery, camera) using a dictionary where each topic maps to related keywords. For instance, keywords like "megapixels," "photo," and "selfie" are mapped to the "camera" topic.

#### Sentiment Analysis

Created a `sentiment` column by applying polarity scores to each comment and headline, classifying them as "Positive," "Negative," or "Neutral."

### Load Stage

After transformation, data was loaded into a MongoDB database named `iphone16_sentiment_analysis` with two collections: `reddit_headlines_collection` and `youtube_comments_collection`. The intermediate and final datasets were also exported as CSV files for backup and offline analysis.

## Orchestrating the ETL Pipeline with Apache Airflow

After executing the ETL pipeline with Apache Airflow, I verified successful task completion via the Airflow webserver GUI. By mapping Docker’s internal port 8080 to my local port 8080, I accessed the GUI at `localhost:8080` in my browser.

- **DAG Overview**: Airflow uses Directed Acyclic Graphs (DAGs) to collect tasks, organize them with dependencies and relationships to specify how they should run. The image below depicts the DAG of this project. The pipeline `extract_Reddit -> clean_Reddit -> transform_Reddit -> analyze_sentiment_Reddit` runs in parallel with the pipeline `extract_YouTube -> clean_YouTube -> transform_YouTube -> analyze_sentiment_YouTube`. The green border lines indicate that all the tasks are executed successfully. The `wait_for_load` task serves as a synchronizing step to ensure that all transformations are complete before loading data. In other words, if the Reddit pipeline finishes first, it will wait for the YouTube pipeline to finish before starting the loading process. 

    ![Figure 1: Airflow Directed Acyclic Graph (DAG)](images/airflow_dag.png)

- **Task Logs**: While green border lines on the Airflow webserver GUI indicate successful task completion, it is important to check task logs to verify success, especially if `try-except` blocks are used in the script. The task log provides precise success or error information. The image below displays the load task log, which shows a successful run with no errors, confirming that the entire ETL pipeline executes smoothly.

    ![Figure 2: Log of the Task load](images/log.png)

## Connecting to the Database with MongoDB Compass

Once the ETL pipeline completed, I accessed the populated MongoDB database `iphone16_sentiment_analysis` in MongoDB Compass. To connect, I used the connection string: `mongodb://{root_username}:{root_password}@host.docker.internal:27017/admin`.

The images below illustrate how the database look like in MongoDB Compass. In the first image, you can see that in the database `iphone16_sentiment_analysis`, there are 2 collections `reddit_headlines` and `youtube_comments`. `reddit_headlines` has 568 documents and `youtube_comments` has around 7,100 documents. Sample documents from each collection are shown in the second and third images. 

![Figure 3: MongoDB Database](images/mongodb_database.png)

![Figure 4: Reddit Collection](images/reddit_collection.png)

![Figure 5: YouTube Collection](images/youtube_collection.png)

## Main Findings

After confirming successful data population, I wrote two queries in MongoDB Query Language (MQL) to generate insights (queries are available in the `query.py` script).

1. **Sentiment Proportion**:

   ```json
   [{"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}]

This query calculates the proportion of sentiments in YouTube comments and Reddit headlines.

2. **Sentiment by Topic**:

   ```json
   [{"$match": {"topics": {"$ne": None}}}, 
    {"$unwind": "$topics"}, 
    {"$group": {"_id": {"topic": "$topics", "sentiment": "$sentiment"}, 
    "count": {"$sum": 1}}}, 
    {"$sort": {"topic": 1, "sentiment": 1}}]

This query calculates sentiment proportions for each topic in both YouTube comments and Reddit headlines.

3. **Insights**

![Figure 6: Reddit Pie Chart](output/reddit_sentiment_pie_chart.png)

![Figure 7: Reddit Heatmap](output/reddit_sentiment_by_topic_heatmap.png)

![Figure 8: YouTube Pie Chart](output/youtube_sentiment_pie_chart.png)

![Figure 9: YouTube Heatmap](output/youtube_sentiment_by_topic_heatmap.png)

- On Reddit, 65.8% of users expressed neutral sentiments towards the iPhone 16, with the most-discussed topics being "camera," "screen size," "battery," and "OS."
- On YouTube, 75.6% of users remained neutral, while the "camera" (573 mentions) and "performance" (179 mentions) were top discussion topics.

These findings align with recent news reports suggesting a lukewarm reception for the iPhone 16, with many users noting limited improvements over previous models, though the upgraded camera quality has been positively highlighted.

## Future Improvements

To enhance the depth and functionality of this project, here are some improvements I plan to explore in the future:

- **Real-Time Data Collection**: Given the high frequency of posts on YouTube and Reddit, it would be beneficial to implement a real-time data streaming pipeline using Apache Kafka. I could create separate Kafka producers for YouTube comments and Reddit headlines, feeding data to these producers in real time. Each producer would then publish data to a dedicated topic within the Kafka cluster (one topic for YouTube and another for Reddit). Additionally, I could set up separate consumers to read from these streams and load the data directly into the MongoDB database. Finally, by connecting the MongoDB database to visualization tools like Tableau or Power BI, I could create interactive, real-time dashboards. However, I need to learn more about setting up Kafka within Docker, as this appears to be a challenging task.

- **Fine-Tuned Sentiment Analysis**: Since the focus of this project is primarily on data engineering rather than data science, I used a relatively simple approach for sentiment analysis. However, a more advanced model, such as BERT or VADER, could capture more nuanced opinions and emotions beyond basic positive, negative, or neutral labels. Implementing this would require additional knowledge in building and fine-tuning NLP models, which I plan to explore as I expand my skill set in natural language processing and machine learning.





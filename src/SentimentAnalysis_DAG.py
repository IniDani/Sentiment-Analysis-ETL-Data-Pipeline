from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

from src.Credentials import YOUTUBE_API_KEY
from src.Credentials import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT

from ETL_Functions.Extract_Functions import (
    Extract_YT_Comments,
    Extract_Reddit_Comments
)
from ETL_Functions.Transform_Functions import (
    YT_Convert_Emoji_to_Text,
    RDT_Convert_Emoji_to_Text,
    YT_HandleHTML,
    RDT_HandleHTML,
    YTcomments_to_Dataframe,
    RDTcomments_to_Dataframe,
    YTcolumn_to_string,
    RDTcolumn_to_string,
    remove_first_character_in_username,
    Sentiment_Analysis,
    YT_Organize_Column,
    RDT_Organize_Column
)
import ETL_Functions.Load_Functions

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20),  # Set a suitable start date
    'retries': 1,
}

# Define the DAG
with DAG(
    'Sentiment Analysis_ETL',
    default_args = default_args,
    description = 'ETL pipeline for sentiment analysis',
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Task: Extract YouTube comments
    def Extract_Comments_from_YouTube():
        from googleapiclient.discovery import build

        youtube = build('youtube', 'v3', developerKey = YOUTUBE_API_KEY)
        video_id = 'FJF0GOaOWk4'

        return Extract_YT_Comments(video_id, youtube)
    
    # Task: Extract Reddit comments
    def Extract_Comments_from_Reddit():
        import praw

        reddit = praw.Reddit(
            client_id = REDDIT_CLIENT_ID,
            client_secret = REDDIT_CLIENT_SECRET,
            user_agent = REDDIT_USER_AGENT
        )
        post_url = 'https://www.reddit.com/r/movies/comments/1ff3566/transformers_one_review_thread/'

        return Extract_Reddit_Comments(post_url, reddit)
    


    # Task: Transform YouTube comments
    def Transform_YouTube_Comments(YT_comments):
        YT_comments = YT_Convert_Emoji_to_Text(YT_comments)
        YT_comments = YT_HandleHTML(YT_comments)

        YT_dataframe = YTcomments_to_Dataframe(YT_comments)
        YT_dataframe = YTcolumn_to_string(YT_dataframe)
        YT_dataframe = remove_first_character_in_username(YT_dataframe)

        # Sentiment Analysis
        YT_dataframe = Sentiment_Analysis(YT_dataframe)
        YT_dataframe = YT_Organize_Column(YT_dataframe)

        return YT_dataframe
    
    # Task: Transform YouTube comments
    def Transform_Reddit_Comments(RDT_comments):
        RDT_comments = RDT_Convert_Emoji_to_Text(RDT_comments)
        RDT_comments = RDT_HandleHTML(RDT_comments)

        RDT_dataframe = RDTcomments_to_Dataframe(RDT_comments)
        RDT_dataframe = RDTcolumn_to_string(RDT_dataframe)
        
        # Sentiment Analysis
        RDT_dataframe = Sentiment_Analysis(RDT_dataframe)
        RDT_dataframe = RDT_Organize_Column(RDT_dataframe)

        return RDT_dataframe
    


    # Task: Combine YouTube and Reddit dataframes
    def Combine_Dataframes(YT_df, RDT_df):
        Combined_Dataframe = pd.concat([YT_df, RDT_df], ignore_index = True)

        return Combined_Dataframe
    


    # Airflow Tasks
    extract_youtube_task = PythonOperator(
        task_id = 'Extract_Comments_from_YouTube'
        python_callable = Extract_Comments_from_YouTube
    )

    extract_reddit_task = PythonOperator(
        task_id = 'Extract_Comments_from_Reddit'
        python_callable = Extract_Comments_from_Reddit
    )

    transform_youtube_task = PythonOperator(
        task_id = 'Transform_YouTube_Comments'
        python_callable = Transform_YouTube_Comments
        op_args = [extract_youtube_task.output]
    )

    transform_reddit_task = PythonOperator(
        task_id = 'Transform_Reddit_Comments'
        python_callable = Transform_Reddit_Comments
        op_args = [extract_reddit_task.output]
    )

    combine_task = PythonOperator(
        task_id = 'Combine_Dataframes'
        python_callable = Combine_Dataframes
        op_args = [transform_youtube_task.output, transform_reddit_task.output]
    )



    # Set Task Dependencies
    [extract_youtube_task, extract_reddit_task] >> [transform_youtube_task, transform_reddit_task] >> combine_task
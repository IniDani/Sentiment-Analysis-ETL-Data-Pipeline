from bs4 import BeautifulSoup
from transformers import pipeline
import html
import emoji
import pandas as pd

# ---|   Functions to convert emojis in comments to text   |---
def YT_Convert_Emoji_to_Text(comments):
  converted_comments = []

  for comment, username, video_title, video_id in comments:
    # Convert emojis in the comment to text descriptions
    converted_comment = emoji.demojize(comment)
    converted_comments.append((converted_comment, username, video_title, video_id))
    
  return converted_comments

def RDT_Convert_Emoji_to_Text(comments):
  converted_comments = []

  for comment_text, username, post_title, post_url in comments:
    # Convert emojis in the comment to text descriptions
    converted_comment = emoji.demojize(comment_text)
    converted_comments.append((converted_comment, username, post_title, post_url))
    
  return converted_comments



# ---|   Functions to handle HTML markups in comments   |---
def YT_HandleHTML(comments):
  cleaned_comments = []

  for comment, username, video_title, video_id in comments:
    # Unescape HTML entities (e.g., &amp; becomes &)
    comment = html.unescape(comment)

    # Remove HTML tags using BeautifulSoup
    soup = BeautifulSoup(comment, "html.parser")
    clean_comment = soup.get_text()

    # Replace line breaks with a space (if necessary)
    clean_comment = clean_comment.replace('\n', ' ').replace('\r', ' ')

    # Strip leading and trailing whitespace
    clean_comment = clean_comment.strip()

    # Add the cleaned comment to the list
    cleaned_comments.append((clean_comment, username, video_title, video_id))

  return cleaned_comments

def RDT_HandleHTML(comments):
  cleaned_comments = []

  for comment_text, username, post_title, post_id in comments:
    # Unescape HTML entities
    comment_text = html.unescape(comment_text)

    # Remove HTML tags using BeautifulSoup
    soup = BeautifulSoup(comment_text, "html.parser")
    clean_comment = soup.get_text()

    # Replace line breaks with a space and strip whitespace
    clean_comment = clean_comment.replace('\n', ' ').replace('\r', ' ').strip()

    # Strip leading and trailing whitespace
    clean_comment = clean_comment.strip()

    # Add the cleaned comment to the list
    cleaned_comments.append((clean_comment, username, post_title, post_id))

  return cleaned_comments



# ---|   Functions to convert the raw data into a dataframe   |---
def YTcomments_to_Dataframe(comments):
  # Create a DataFrame with 'Comment', 'Username', and 'Video Title' columns
  df = pd.DataFrame(comments, columns = ['Comment', 'Username', 'Video Title', 'Video ID'])

  # Add an index column for easier reference
  df.reset_index(inplace = True)
  df.rename(columns = {'index': 'ID'}, inplace = True)

  return df

def RDTcomments_to_Dataframe(comments):
  # Create a DataFrame with 'Comment', 'Username', and 'Post Title' columns
  df = pd.DataFrame(comments, columns = ['Comment', 'Username', 'Post Title', 'Post URL'])

  # Add an index column for easier reference
  df.reset_index(inplace = True)
  df.rename(columns = {'index': 'ID'}, inplace = True)

  return df



# ---|   Functions to convert columns to string data type   |---
def YTcolumn_to_string(df):
  df['Comment'] = df['Comment'].astype(str)
  df['Username'] = df['Username'].astype(str)
  df['Video Title'] = df['Video Title'].astype(str)
  df['Video ID'] = df['Video ID'].astype(str)
  
  return df

def RDTcolumn_to_string(df):
  df['Comment'] = df['Comment'].astype(str)
  df['Username'] = df['Username'].astype(str)
  df['Post Title'] = df['Post Title'].astype(str)
  df['Post URL'] = df['Post URL'].astype(str)
  
  return df



# ---|   Functions to remove first character in YouTube username   |---
def remove_first_character_in_username(df):
  df['Username'] = df['Username'].str.slice(1)  # First character is always "@"

  return df



# ---|   Functions for Sentiment Analysis   |---
def Sentiment_Analysis(df):
  sentiment_analyzer = pipeline("sentiment-analysis", model = "distilbert-base-uncased-finetuned-sst-2-english")

  # Create a new column 'Sentiment' initialized with empty strings
  df['Sentiment'] = ""
  df['Sentiment Score'] = ""

  # Iterate through the DataFrame and perform sentiment analysis
  for index, row in df.iterrows():
    # Get the comment text
    text = row['Comment']
    
    # Truncate the text to the maximum sequence length
    max_length = 512  # Adjust if needed based on the model's limit
    if len(text) > max_length:
      text = text[:max_length]

    # Perform sentiment analysis
    result = sentiment_analyzer(text)[0]
    label = result['label']
    score = result['score']

    # Assign sentiment label with a custom threshold for Neutral
    if score > 0.6:
      sentiment = "Positive"
    elif score < 0.4:
      sentiment = "Negative"
    else:
      sentiment = "Neutral"

    # Update the column with the sentiment label
    df.at[index, 'Sentiment'] = sentiment
    df.at[index, 'Sentiment Score'] = score
  
  return df
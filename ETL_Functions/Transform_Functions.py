from bs4 import BeautifulSoup
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
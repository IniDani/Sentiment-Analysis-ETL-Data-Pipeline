import logging

# Function to get the next YouTube video ID from the file
def Get_Next_Video_ID():
    videoIDpath = '/home/DanFaRa/airflow/dags/resources/video_ids.txt'
    try:
        with open(videoIDpath, 'r') as file:
            video_ids = file.readlines()
        if not video_ids:
            raise ValueError("No more video IDs to process.")
        
        next_video_id = video_ids[0].strip()  # Get the first video ID

        # Update the file by removing the processed ID
        with open(videoIDpath, 'w') as file:
            file.writelines(video_ids[1:])  # Remove the first line
        
        logging.info(f"Next video ID: {next_video_id}")
        return next_video_id
    except FileNotFoundError:
        logging.error(f"The file {videoIDpath} does not exist.")
        raise
    except Exception as e:
        logging.error(f"Error in Get_Next_Video_ID: {e}")
        raise

# Function to get the next Reddit post URL from the file
def Get_Next_Reddit_Post_URL():
    redditURLpath = '/home/DanFaRa/airflow/dags/resources/redditpost_url.txt'
    try:
        with open(redditURLpath, 'r') as file:
            reddit_urls = file.readlines()

        if not reddit_urls:
            raise ValueError("No more Reddit URLs to process.")
        
        next_reddit_url = reddit_urls[0].strip()  # Get the first Reddit post URL
        
        # Update the file by removing the processed URL
        with open(redditURLpath, 'w') as file:
            file.writelines(reddit_urls[1:])  # Remove the first line
        
        logging.info(f"Next post URL: {next_reddit_url}")
        return next_reddit_url
    except FileNotFoundError:
        logging.error(f"The file {redditURLpath} does not exist.")
        raise
    except Exception as e:
        logging.error(f"Error in Get_Next_Reddit_Post_ID: {e}")
        raise



# Function to Extract Comments from a YouTube video using YouTube API
def Extract_YT_Comments(video_id, youtube):
  comments = []
  next_page_token = None

  # Video Title
  video_request = youtube.videos().list(
      part = 'snippet',
      id = video_id
  )
  video_response = video_request.execute()
  video_title = video_response['items'][0]['snippet']['title']

  # Video Comments and Username
  while True:
    request = youtube.commentThreads().list(
        part = 'snippet',
        videoId = video_id,
        pageToken = next_page_token,
        maxResults = 100
    )
    response = request.execute()

    for item in response['items']:
      comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
      username = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
      comments.append((comment, username, video_title, video_id))

    next_page_token = response.get('nextPageToken')
    if not next_page_token:
      break

  return comments



# Function to Extract Comments from a Reddit Post
def Extract_Reddit_Comments(post_url, reddit_client):
  # Initialize a list to store the data
  comments_data = []

  # Get the submission (post) from the provided URL
  submission = reddit_client.submission(url = post_url)

  # Extract the post title
  post_title = submission.title

  # Replace "More Comments" to simplify and fetch all comments
  submission.comments.replace_more(limit = 0)

  # Iterate through each comment and collect the data
  for comment in submission.comments.list():
    # Extract the comment text, post title, and username
    comment_text = comment.body
    username = comment.author.name if comment.author else "Anonymous"
    comments_data.append((comment_text, username, post_title, post_url))

  return comments_data

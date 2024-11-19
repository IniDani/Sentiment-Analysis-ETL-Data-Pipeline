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
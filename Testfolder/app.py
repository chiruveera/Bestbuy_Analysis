# app.py
import praw
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("REDDIT_USER_AGENT")
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

# Basic validation
if not all([CLIENT_ID, CLIENT_SECRET, USER_AGENT, USERNAME, PASSWORD]):
    print("Error: Missing one or more Reddit API credentials in .env file.")
    print("Please ensure REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, REDDIT_USERNAME, and REDDIT_PASSWORD are set.")
    exit()

# Initialize the Reddit instance (authenticate)
try:
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT,
        username=USERNAME,
        password=PASSWORD
    )

    # Attempt to fetch your own user details to verify the connection
    me = reddit.user.me()
    print("PRAW initialized and connected successfully!")
    print(f"Logged in as: {me.name}")

    # --- NEW CODE TO FETCH AND PRINT SAMPLE DATA ---

    # 1. Define the subreddit to target
    # Choose a subreddit where you expect product reviews/discussions
    subreddit_name = "BuyItForLife" # Example subreddit

    print(f"\n--- Fetching top 5 posts from r/{subreddit_name} ---")

    # 2. Get the subreddit object
    subreddit = reddit.subreddit(subreddit_name)

    # 3. Fetch a few "hot" or "new" submissions (posts)
    # The project plan mentions "user-generated content" and "authentic, unstructured user opinions" 
    # For reviews, 'new' or 'hot' might be more relevant than 'top' for current discussions.
    for submission in subreddit.hot(limit=5): # Fetching 5 hot posts as a sample
        print(f"\nSubmission ID: {submission.id}")
        print(f"Title: {submission.title}")
        print(f"URL: {submission.url}")
        print(f"Score: {submission.score}")
        print(f"Number of Comments: {submission.num_comments}")
        print(f"Selftext (first 200 chars): {submission.selftext[:200] if submission.selftext else '[No selftext]'}") # This is where the main body of a text post is
        print("-" * 30)

        # Optional: Fetch and print a few comments from each post
        # These comments might contain the "reviews" or opinions you're looking for
        # Be mindful of rate limits if you fetch too many comments for too many posts
        if submission.num_comments > 0:
            print(f"--- Top 2 comments for '{submission.title[:50]}...' ---")
            submission.comments.replace_more(limit=0) # Expand 'More Comments' links
            for i, comment in enumerate(submission.comments.list()):
                if i >= 2: # Limit to top 2 comments for sample
                    break
                print(f"  Comment by u/{comment.author.name if comment.author else '[deleted]'}: {comment.body[:150]}...") # Truncate comment body
                print("  " + "-" * 28)


except Exception as e:
    print(f"An error occurred during API interaction: {e}")
    print("Please double-check your .env file credentials and ensure you have network access.")

    
# app.py (Updated section)

# ... (previous PRAW initialization code) ...

# --- CODE FOR SPRINT 2: FULL DATA ACQUISITION & BASIC STORAGE ---

import json
import time # For adding delays
import pandas as pd # For easy data handling and saving

# Define subreddits to target
target_subreddits = [
    "BuyItForLife",
    "SkincareAddiction",
    "buildapc",
    # Add more relevant subreddits here if you find them, e.g.,
    # "malefashionadvice",
    # "femalefashionadvice",
    # "electronics"
]

# Set a target for total submissions across all subreddits
# Aim for approximately 40,000 posts total
num_submissions_to_fetch_per_subreddit = 5000 # Adjust this to get ~40K overall (e.g., 8 subreddits * 5000 posts)
comments_per_post_limit = 10 # Increase this if you need more comments per post

print(f"\n--- Starting bulk data acquisition from {len(target_subreddits)} subreddits ---")

all_posts_data = []
all_comments_data = []

for subreddit_name in target_subreddits:
    print(f"\nFetching from r/{subreddit_name}...")
    subreddit = reddit.subreddit(subreddit_name)
    fetched_count_current_subreddit = 0

    # Using .top() with time_filter='all' is often better for large historical datasets
    # If you only want recent posts, stick with .new() but be aware of its depth limit
    for submission in subreddit.top(time_filter='all', limit=None): # Use 'top' for large historical volume
        if fetched_count_current_subreddit >= num_submissions_to_fetch_per_subreddit:
            print(f"Reached {num_submissions_to_fetch_per_subreddit} submissions for r/{subreddit_name}. Moving to next.")
            break

        post_data = {
            'id': submission.id,
            'title': submission.title,
            'url': submission.url,
            'score': submission.score,
            'num_comments': submission.num_comments,
            'created_utc': submission.created_utc,
            'author': submission.author.name if submission.author else '[deleted]',
            'subreddit': subreddit_name, # Add subreddit name to data
            'selftext_raw': submission.selftext if submission.selftext else '',
            'selftext_cleaned_basic': submission.selftext.strip() if submission.selftext else ''
        }
        all_posts_data.append(post_data)

        if submission.num_comments > 0:
            submission.comments.replace_more(limit=0)
            for i, comment in enumerate(submission.comments.list()):
                if i >= comments_per_post_limit: # Limit comments per post
                    break
                comment_data = {
                    'comment_id': comment.id,
                    'submission_id': submission.id,
                    'author': comment.author.name if comment.author else '[deleted]',
                    'body_raw': comment.body,
                    'body_cleaned_basic': comment.body.strip(),
                    'score': comment.score,
                    'created_utc': comment.created_utc,
                    'subreddit': subreddit_name # Add subreddit name to comments
                }
                all_comments_data.append(comment_data)

        fetched_count_current_subreddit += 1
        if fetched_count_current_subreddit % 100 == 0: # Print progress every 100 posts
            print(f"  Fetched {fetched_count_current_subreddit} submissions from r/{subreddit_name}...")
        
        # Add a polite delay after each submission fetch, especially for .top(limit=None)
        time.sleep(1) # Sleep for 1 second per submission to avoid hitting rate limits too hard

print(f"\nFinished fetching from all subreddits.")
print(f"Total submissions fetched: {len(all_posts_data)}")
print(f"Total comments fetched: {len(all_comments_data)}")

# Save data
posts_df = pd.DataFrame(all_posts_data)
comments_df = pd.DataFrame(all_comments_data)

try:
    posts_df.to_csv('reddit_posts_raw_bulk.csv', index=False) # Changed filename to avoid overwrite
    comments_df.to_csv('reddit_comments_raw_bulk.csv', index=False) # Changed filename
    print("Raw Reddit posts and comments saved to CSV files.")
    print("Sample of saved posts:")
    print(posts_df.head())
    print("\nSample of saved comments:")
    print(comments_df.head())

except Exception as e:
    print(f"Error saving data: {e}")
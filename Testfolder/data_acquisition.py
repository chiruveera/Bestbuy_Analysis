# data_acquisition.py
import time
import pandas as pd
from reddit_api_client import get_reddit_instance # Import your PRAW instance getter

def collect_reddit_data(subreddit_name, num_submissions):
    """
    Collects a specified number of submissions and their comments from a subreddit.
    Handles basic pagination and includes polite delays.
    """
    try:
        reddit = get_reddit_instance()
        print("PRAW instance obtained for data collection.")
        # You can test login again here if needed, but it should be handled by get_reddit_instance
        # me = reddit.user.me()
        # print(f"Logged in as: {me.name}")

        print(f"\n--- Starting data acquisition from r/{subreddit_name} ---")
        print(f"Attempting to fetch {num_submissions} submissions and their top comments.")

        subreddit = reddit.subreddit(subreddit_name)
        all_posts_data = []
        all_comments_data = []

        fetched_count = 0
        for submission in subreddit.new(limit=None):
            if fetched_count >= num_submissions:
                break

            post_data = {
                'id': submission.id,
                'title': submission.title,
                'url': submission.url,
                'score': submission.score,
                'num_comments': submission.num_comments,
                'created_utc': submission.created_utc,
                'author': submission.author.name if submission.author else '[deleted]',
                'selftext_raw': submission.selftext if submission.selftext else '',
                'selftext_cleaned_basic': submission.selftext.strip() if submission.selftext else ''
            }
            all_posts_data.append(post_data)

            if submission.num_comments > 0:
                submission.comments.replace_more(limit=0)
                for i, comment in enumerate(submission.comments.list()):
                    if i >= 5:
                        break
                    comment_data = {
                        'comment_id': comment.id,
                        'submission_id': submission.id,
                        'author': comment.author.name if comment.author else '[deleted]',
                        'body_raw': comment.body,
                        'body_cleaned_basic': comment.body.strip(),
                        'score': comment.score,
                        'created_utc': comment.created_utc
                    }
                    all_comments_data.append(comment_data)

            fetched_count += 1
            if fetched_count % 10 == 0:
                time.sleep(5) # Polite delay
                print(f"Fetched {fetched_count} submissions...")

        print(f"\nFinished fetching. Total submissions fetched: {len(all_posts_data)}")
        print(f"Total comments fetched: {len(all_comments_data)}")

        posts_df = pd.DataFrame(all_posts_data)
        comments_df = pd.DataFrame(all_comments_data)

        try:
            posts_df.to_csv('reddit_posts_raw.csv', index=False)
            comments_df.to_csv('reddit_comments_raw.csv', index=False)
            print("Raw Reddit posts and comments saved to CSV files.")
            print("Sample of saved posts:")
            print(posts_df.head())
            print("\nSample of saved comments:")
            print(comments_df.head())

        except Exception as e:
            print(f"Error saving data: {e}")

    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred during data collection: {e}")
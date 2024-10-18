import json
import os
import pytz
from datetime import datetime, timedelta
from ntscraper import Nitter
import pandas as pd
from dotenv import load_dotenv

 
load_dotenv()
username=json.loads(os.getenv("TWITTER_USERS").replace("'", '"'))

scraper = Nitter(log_level=0, skip_instance_check=False)
scraper.instance = "https://nitter.woodland.cafe/"

def fetch_24_hour_tweets(users):
    timezone = pytz.timezone("UTC")  
    current_time = datetime.now(timezone)
    last_24_hours = current_time - timedelta(hours=24)

    for user in users:
        print(f"Fetching tweets for user: {user}")
        try:
            tweets = scraper.get_tweets(user, mode="user")
        except Exception as e:
            print(f"Error fetching tweets for user {user}: {e}")
            continue
        
        if tweets['tweets']:
            data = {
                'text': [],
                'date': [],
                'retweets': [],
                'quoted-tweets': []
            }

            for tweet in tweets['tweets']:
                tweet_time = datetime.strptime(tweet['date'], '%b %d, %Y · %I:%M %p %Z').replace(tzinfo=pytz.utc).astimezone(timezone)
                if tweet_time >= last_24_hours:
                    data['text'].append(tweet['text'])
                    data['date'].append(tweet_time.strftime('%Y-%m-%d %H:%M:%S %Z%z'))
                    
                    
                    if tweet['is-retweet']:
                        data['retweets'].append(tweet['link']) 
                    else:
                        data['retweets'].append("No retweet")

                    
                    if 'quoted-post' in tweet:
                        quoted_info = tweet['quoted-post']
                        quoted_text=quoted_info.get('text', 'No text available')
                        data['quoted-tweets'].append(quoted_text)
                    else:
                        data['quoted-tweets'].append("No quoted tweet")

            
        else:
            print(f"No tweets were found for user: {user}.")
    df=pd.DataFrame(data)
    print(df)  
    df.to_csv("user_tweet_data.csv")
fetch_24_hour_tweets(username)

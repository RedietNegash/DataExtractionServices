import pandas as pd




content_email=pd.read_csv("user_email_data.csv")
content_email['total_content'] = content_email['from']+ content_email['title'] + content_email['content']
content_t = content_email[['total_content']]



content_tweet=pd.read_csv("user_tweet_data.csv")
content_tweet['total_content'] = content_tweet['text'] + content_tweet['retweets'] + content_tweet['quoted-tweets']
content_g = content_tweet[['total_content']]




df = pd.merge(content_t, content_g, on='total_content')
print(df.head())



# def clean(item):
#     import re
#     replace('')

# embedding = df['total'].map(lambda x:clean)
# embedding = df['total'].tolist()


# # embeding model
# from sentence_transformers import SentenceTransformer

# model = SentenceTransformer("all-MiniLM-L6-v2")
# embedding = model.

# df['embedding'] = embedding


# print(content_email.head())
# print(content_tweet.head())


# from sentence_transformers import SentenceTransformer

# model = SentenceTransformer("all-MiniLM-L6-v2")


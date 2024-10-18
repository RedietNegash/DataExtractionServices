import pandas as pd
import unicodedata
import re
import string
from nltk.corpus import stopwords
from sentence_transformers import SentenceTransformer


def process_text(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = text.lower()
    stop_words = set(stopwords.words('english'))
    text = " ".join([word for word in text.split() if word not in stop_words])
    return text


content_email = pd.read_csv("user_email_data.csv")
content_tweet = pd.read_csv("user_tweet_data.csv")


content_email['total_content'] = content_email['from'] + content_email['title'] + content_email['content']
content_tweet['total_content'] = content_tweet['text'] + content_tweet['retweets'] + content_tweet['quoted-tweets']


df = pd.concat([content_email[['total_content']], content_tweet[['total_content']]], axis=0).reset_index(drop=True)


df['processed_content'] = df['total_content'].apply(process_text)


model = SentenceTransformer('all-MiniLM-L6-v2')

df['processed_content'] = df['processed_content'].fillna('')
processed_content = df['processed_content'].tolist()
all_embeddings = model.encode(processed_content)
df['embedding'] = list(all_embeddings)
# imortsetup.qdrznt
# qdrznt= Qdrznt().batch_insert_data(df[['id','embedding','processed_contetn']])


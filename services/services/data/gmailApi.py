import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
from bs4 import BeautifulSoup
from base64 import urlsafe_b64decode
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()


prompt = (
    "You are given the plain text content of more than one newsletter email. Your task is to extract and return only "
    "the title and the first paragraph of the main news content, ensuring that all ads and sponsored content are removed. "
    "Since the email contains more than one newsletter, you must extract all of them. Follow these steps:\n"
    "    1. Identify the title of the main news content.\n"
    "    2. Extract the first paragraph of the main news content.\n"
    "    3. Remove any ads, sponsored content, or unrelated promotional material.\n"
    "    4. Return a list of all news items, with the output format for each news being:\n"
    '        "Content: {content}\n'
    '        "----------------------"\n'
    "    Return the output as plain text."
)

def gmail_authenticate():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', ['https://www.googleapis.com/auth/gmail.readonly'])
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)

service = gmail_authenticate()

def search_messages(service, query):
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages

def get_content(text):
    api_key = os.getenv("GOOGLE_API_KEY")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name="gemini-1.5-flash")
    response = model.generate_content([prompt, text])
    content = response.text
    print(content)
    return content

def parse_parts(service, parts):
    if not parts: 
        return ''
    
    text = ''
    for part in parts:
        mimeType = part.get("mimeType")
        body = part.get("body")
        data = body.get("data")
        if part.get("parts"):
            text += parse_parts(service, part.get("parts"))
        if mimeType == "text/plain":
            if data:
                text += urlsafe_b64decode(data).decode()
        elif mimeType == "text/html":
            if data:
                text += urlsafe_b64decode(data).decode()
                text = BeautifulSoup(text, "html.parser").get_text()
    return text

def read_message(service, message):
    msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
    payload = msg['payload']
    headers = payload.get("headers")
    parts = payload.get("parts")
    email_data = {
        'from': '',
        'title': '',
        'content': ''
    }
    if headers:
        for header in headers:
            name = header.get("name")
            value = header.get("value")
            if name.lower() == 'from':
                email_data['from'] = value
            elif name.lower() == 'subject':
                email_data['title'] = value
    text = parse_parts(service, parts)
    email_data['content'] = get_content(text)
    return email_data

results = search_messages(service, "from: Microsoft")
print(f"Found {len(results)} results.")
emails = []
for index, msg in enumerate(results):
    email_data = read_message(service, msg)
    email_data['index'] = index
    emails.append(email_data)
print(emails[0])


df = pd.DataFrame(emails, columns=['index', 'from', 'title', 'content'])
df.to_csv("user_email_data.csv", index=False)
print(df)

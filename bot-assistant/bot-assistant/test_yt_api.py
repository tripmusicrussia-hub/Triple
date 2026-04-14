"""Проверка что YT API credentials рабочие."""
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

creds_data = {}
with open("yt_token.txt", "r", encoding="utf-8") as f:
    for line in f:
        k, _, v = line.strip().partition("=")
        creds_data[k] = v

creds = Credentials(
    token=None,
    refresh_token=creds_data["YT_REFRESH_TOKEN"],
    client_id=creds_data["YT_CLIENT_ID"],
    client_secret=creds_data["YT_CLIENT_SECRET"],
    token_uri="https://oauth2.googleapis.com/token",
    scopes=["https://www.googleapis.com/auth/youtube.readonly"],
)

yt = build("youtube", "v3", credentials=creds)
resp = yt.channels().list(part="snippet,statistics", mine=True).execute()
ch = resp["items"][0]
print("CHANNEL OK:", ch["snippet"]["title"])
print("  subs:", ch["statistics"].get("subscriberCount"))
print("  views:", ch["statistics"].get("viewCount"))
print("  videos:", ch["statistics"].get("videoCount"))

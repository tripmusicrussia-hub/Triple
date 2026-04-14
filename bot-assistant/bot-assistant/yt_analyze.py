"""Выгрузка всех видео канала + ранжирование по views."""
import json
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

ch = yt.channels().list(part="contentDetails", mine=True).execute()
uploads_id = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

video_ids = []
next_page = None
while True:
    pl = yt.playlistItems().list(
        part="contentDetails", playlistId=uploads_id, maxResults=50, pageToken=next_page
    ).execute()
    for it in pl["items"]:
        video_ids.append(it["contentDetails"]["videoId"])
    next_page = pl.get("nextPageToken")
    if not next_page:
        break

print(f"Total video IDs fetched: {len(video_ids)}")

videos = []
for i in range(0, len(video_ids), 50):
    chunk = video_ids[i:i+50]
    resp = yt.videos().list(
        part="snippet,statistics,contentDetails", id=",".join(chunk)
    ).execute()
    for v in resp["items"]:
        sn = v["snippet"]
        st = v["statistics"]
        videos.append({
            "id": v["id"],
            "title": sn["title"],
            "published": sn["publishedAt"][:10],
            "views": int(st.get("viewCount", 0)),
            "likes": int(st.get("likeCount", 0)),
            "comments": int(st.get("commentCount", 0)),
            "duration": v["contentDetails"]["duration"],
            "tags": sn.get("tags", []),
            "description": sn.get("description", ""),
        })

videos.sort(key=lambda x: x["views"], reverse=True)

with open("yt_videos_dump.json", "w", encoding="utf-8") as f:
    json.dump(videos, f, ensure_ascii=False, indent=2)

print(f"\nTOP 10 by views:")
for v in videos[:10]:
    print(f"  {v['views']:>6}  {v['published']}  {v['title'][:70]}")

print(f"\nBOTTOM 10 by views:")
for v in videos[-10:]:
    print(f"  {v['views']:>6}  {v['published']}  {v['title'][:70]}")

total_views = sum(v["views"] for v in videos)
over_100 = sum(1 for v in videos if v["views"] >= 100)
over_1000 = sum(1 for v in videos if v["views"] >= 1000)
under_20 = sum(1 for v in videos if v["views"] < 20)
print(f"\nSTATS:")
print(f"  total videos: {len(videos)}")
print(f"  total views: {total_views}")
print(f"  >= 1000 views: {over_1000}")
print(f"  >= 100 views: {over_100}")
print(f"  < 20 views: {under_20}")

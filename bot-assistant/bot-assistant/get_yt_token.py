"""
Один раз локально: получает refresh_token для YouTube Data API.

1. pip install google-auth-oauthlib google-api-python-client
2. python get_yt_token.py
3. Откроется браузер → залогинься в Google под аккаунтом владельца канала @IIIPLFIII
4. На warning screen «Google hasn't verified this app» → Advanced → Go to TRIPLE FILL Bot (unsafe)
5. Consent → скрипт распечатает 3 значения для Render env vars
"""

from __future__ import annotations

import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = "558047224449-qmbu6lnhdvn6i2nm29kldurvjjv8jflm.apps.googleusercontent.com"
CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET", "")

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def main() -> None:
    if not CLIENT_SECRET:
        print("ОШИБКА: передай YT_CLIENT_SECRET через env.")
        print("Windows PowerShell:  $env:YT_CLIENT_SECRET='GOCSPX-...'; python get_yt_token.py")
        print("bash:               YT_CLIENT_SECRET='GOCSPX-...' python get_yt_token.py")
        sys.exit(1)

    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    out_path = "yt_token.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"YT_CLIENT_ID={CLIENT_ID}\n")
        f.write(f"YT_CLIENT_SECRET={CLIENT_SECRET}\n")
        f.write(f"YT_REFRESH_TOKEN={creds.refresh_token}\n")
    print(f"OK. Tokens written to {out_path}")


if __name__ == "__main__":
    main()

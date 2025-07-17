import sqlite3
from yt_dlp import YoutubeDL

# Base script to populate the DATABASE - Test

conn =  sqlite3.connect("yt_test.db")
cursor = conn.cursor()

# Replace this with the full channel URL or channel ID
channel_url = "https://www.youtube.com/@ALLaboutVal"

# yt-dlp options
ydl_opts = {
    "extract_flat": "in_playlist",  # Don't download videos, just extract metadata
    "skip_download": True,  # Definitely don't download
    "quiet": True,  # Less output
}

with YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(channel_url, download=False)
    
    if "id" in info:
        cursor.execute(
            """INSERT INTO channels VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                info.get("id"),
                info.get("title"),
                info.get("channel_url"),
                info.get("channel_id"),
                info.get("uploader_id"),
                info.get("uploader_url"),
                info.get("channel_follower_count")
            ),
        )

    print("Data inserted into the table")
    cursor.execute("SELECT * FROM channels")
    for row in cursor.fetchall():
        print(row)
        
    if "entries" in info:
        for entry in info["entries"]:
            if entry['title'] == info.get("title") + " - Videos":
                for video in entry['entries']:
                    cursor.execute(
                        """INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                        (
                            video.get("id"),
                            info.get("channel_id"),
                            video.get("url"),
                            video.get("title"),
                            video.get("description"),
                            video.get("duration"),
                            video.get("view_count")
                        )
                    )
        

conn.commit()

conn.close()

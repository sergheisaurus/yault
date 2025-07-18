import json
from yt_dlp import YoutubeDL

def get_video(id):
        
    url = f"https://www.youtube.com/@ValeriyaASMR"
    
    opts = {
        "extract_flat": True,
        "skip_download": True,
        "quiet": True,
        "cookiefile": "cookies.txt"
    }
    
    with YoutubeDL(opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception:
            raise Exception
        
        with open("video_2.json", "w") as outfile:
            json.dump(info, outfile, indent=4)

get_video("0dqpWQVEj6E")
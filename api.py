import json
import sqlite3
import asyncio
from typing import Optional
from fastapi import FastAPI, HTTPException
from yt_dlp import YoutubeDL
from pydantic import BaseModel
from thumbnail import Thumbnail
from contextlib import asynccontextmanager

channel_queue = asyncio.Queue()
processing_channel: Optional[str] = None
done_channels = []
failed_channels = []

video_queue = asyncio.Queue()
processing_video: Optional[str] = None
done_videos = []
failed_videos = []

lock = asyncio.Lock()

con = sqlite3.connect("yt_test.db")
cur = con.cursor()

class Channel(BaseModel):
    id: str
    title: str
    url: str
    subscribers: int

class ObjectID(BaseModel):
    id: str

class Video(BaseModel):
    id: str
    channel_id: str
    url: str
    title: str
    description: str
    duration: int
    view_count: Optional[int] = None
    thumbnails: dict

async def add_channel_to_db(id):
    
    if id[0] != "@":
        id = f"@{id}"
        
    url = f"https://www.youtube.com/{id}"
    
    opts = {
        "extract_flat": "in_playlist",
        "skip_download": True,
        "quiet": True
    }
    
    with YoutubeDL(opts) as ydl:
        try:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)
        except Exception:
            raise Exception
        
        if not info:
            # Handle the case where info is None or empty
            raise Exception("Failed to extract info or got None")

        # if "id" in info:
        #     cur.execute(
        #         """INSERT INTO channels VALUES (?, ?, ?, ?, ?, ?, ?)""",
        #         (
        #             info.get("id"),
        #             info.get("title"),
        #             info.get("channel_url"),
        #             info.get("channel_id"),
        #             info.get("uploader_id"),
        #             info.get("uploader_url"),
        #             info.get("channel_follower_count")
        #         ),
        #     )
        #     con.commit()

    print("Data inserted into the table")

    if "entries" in info:
        for entry in info["entries"]:
            if "entries" in entry:
                title = info.get("title") or ""
                if entry['title'] == title + " - Videos":
                    for video in entry['entries']:
                        cur.execute(
                            """INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                            (
                                video.get("id"),
                                info.get("uploader_id"),
                                video.get("url"),
                                video.get("title"),
                                video.get("description"),
                                video.get("duration"),
                                video.get("view_count")
                            )
                        )
                    con.commit()
            else:
                cur.execute(
                    """INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                    (
                        entry.get("id"),
                        info.get("uploader_id"),
                        entry.get("url"),
                        entry.get("title"),
                        entry.get("description"),
                        entry.get("duration"),
                        entry.get("view_count")
                    )
                )
                con.commit()
                
        title = info.get("title") or ""
        print(title + " - " + str(info.get("channel_follower_count")))

async def process_channel_queue():
    global processing_channel
    while True:
        channel_id = await channel_queue.get()
        async with lock:
            processing_channel = channel_id
        try:
            await add_channel_to_db(channel_id)  # your real add function
            async with lock:
                done_channels.append(channel_id)
        except Exception as e:
            print(f"Failed to add channel {channel_id}: {e}")
            async with lock:
                failed_channels.append({"id": channel_id, "error": str(e)})
        finally:
            async with lock:
                processing_channel = None
            channel_queue.task_done()

async def process_video_queue():
    global processing_video
    while True:
        video_id = await video_queue.get()
        async with lock:
            processing_video = video_id
        try:
            await get_video_details(video_id)
            async with lock:
                done_videos.append(video_id)
        except Exception as e:
            print(f"Failed to add video {video_id}: {e}")
            async with lock:
                failed_videos.append({"id": video_id, "error": str(e)})
        finally:
            async with lock:
                processing_video = None
            video_queue.task_done()

async def get_video_details(id: str):
    url = f"https://www.youtube.com/watch?v={id}"
    
    opts = {
        "extract_flat": True,
        "skip_download": True,
        "quiet": True,
        "cookiefile": "cookies.txt"
    }

    with YoutubeDL(opts) as ydl:
        try:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)
        except Exception:
            raise Exception("Failed to extract YouTube info")

        uploader_id = info.get("uploader_id")
        print(f"Uploader ID: {uploader_id}")

        # Try to retrieve the channel first
        channel_exists = True
        try:
            await get_channel_by_id(uploader_id)
        except HTTPException as e:
            print(f"❌ Channel not found: {e.detail}")
            await channel_queue.put(uploader_id)
            channel_exists = False
        except Exception as e:
            print(f"⚠️ Unexpected channel fetch error: {str(e)}")
            channel_exists = False

        # Then try to check if the video already exists
        try:
            await get_video_by_id(id)
            print(f"✅ Video {id} already exists")
            return
        except HTTPException:
            pass  # Proceed to add it
        except Exception as e:
            print(f"⚠️ Unexpected video fetch error: {str(e)}")

        # Add the video
        video = Video(
            id=info.get("id"),
            channel_id=info.get("uploader_id"),
            url=info.get("webpage_url"),
            title=info.get("title"),
            description=info.get("description"),
            duration=info.get("duration"),
            view_count=info.get("view_count"),
            thumbnails=Thumbnail(id).as_dict()
        )

        return await add_video_to_db(video)

async def add_video_to_db(video: Video):
    
    cur.execute(
        "INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)",
        (video.id, video.channel_id, video.url, video.title, video.description, video.duration, video.view_count,)
    )
    con.commit()

    return video

@asynccontextmanager
async def lifespan(app: FastAPI):
    channel_task = asyncio.create_task(process_channel_queue())
    video_task = asyncio.create_task(process_video_queue())

    yield  # App runs here

    channel_task.cancel()
    video_task.cancel()
    try:
        await channel_task
    except asyncio.CancelledError:
        pass
    try:
        await video_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)


@app.get("/channel/{id}", response_model=Channel)
async def get_channel_by_id(id: str):
    try:
        channel_id = ObjectID(id=id)
        res = cur.execute(
            "SELECT id, title, uploader_url, channel_follower_count FROM channels WHERE uploader_id = (?)",
            (channel_id.id,)
        )
        row = res.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_id.id}' not found")

        return Channel(id=row[0], title=row[1], url=row[2], subscribers=row[3])

    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {str(e)}")

@app.get("/channel/{id}/videos")
async def get_videos_by_channel(id: str, limit: int = 10, page: int = 1):
    channel_id = ObjectID(id = id)
    if page <= 1:
        page = 0
    else:
        page -= 1

    res = cur.execute(
        "SELECT id, channel_id, url, title, description, duration, view_count FROM videos WHERE channel_id = (?) LIMIT (?) OFFSET (?)",
        (channel_id.id, limit, page,)
    )
    
    rows = res.fetchall()

    videos = [Video(id = row[0], channel_id=ObjectID(id=row[1]).id, url=row[2], title=row[3], description=row[4], duration=row[5], view_count=row[6], thumbnails=Thumbnail(row[0]).as_dict()) for row in rows]

    return videos

@app.get("/channels")
async def get_all_channels():
    
    res = cur.execute("SELECT id, title, uploader_url, channel_follower_count FROM channels")
    rows = res.fetchall()
    
    if len(rows) < 1:
        return {"There are no channels available"}
    
    channels = [Channel(id=row[0], title=row[1], url=row[2], subscribers=row[3]) for row in rows]
    
    return channels

@app.get("/channels/search/")
async def get_channels_search(q: str = ""):
    
    res = cur.execute(
        "SELECT id, title, uploader_url, channel_follower_count FROM channels WHERE title LIKE ?",
        (f"%{q}%",)
    )
    
    rows = res.fetchall()
    
    if len(rows) < 1:
        return {f"There are no channels found with {q}"}
    
    channels = [
        Channel(id=row[0], title=row[1], url=row[2], subscribers=row[3]) for row in rows
    ]
    
    return channels

@app.get("/channels/add/")
async def add_channel(id: str):
    channel_id = ObjectID(id=id)
    
    await channel_queue.put(channel_id.id)
    return {"message": f"Channel {channel_id.id} added to the queue"}

@app.get("/channels/status/")
async def channels_status():
    async with lock:
        waiting = list(channel_queue._queue)
        current = processing_channel
        finished = done_channels.copy()
        failed = failed_channels.copy()
    return {
        "waiting": waiting,
        "processing": current,
        "done": finished,
        "failed": failed,
    }

@app.get("/video/{id}")
async def get_video_by_id(id: str):
    try:
        video_id = ObjectID(id=id)
        
        res = cur.execute(
            "SELECT id, channel_id, url, title, description, duration, view_count FROM videos WHERE id = (?)",
            (video_id.id,)
        )
        
        row = res.fetchone()
        
        if row is None:
            raise HTTPException(status_code=404, detail=f"Video '{video_id.id}' not found")
        
        return Video(id = row[0], channel_id=ObjectID(id=row[1]).id, url=row[2], title=row[3], description=row[4], duration=row[5], view_count=row[6], thumbnails=Thumbnail(row[0]).as_dict())
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request: {str(e)}")

@app.get("/videos/add/")
async def add_video_to_queue(id: str):
    video_id = ObjectID(id=id)
    await video_queue.put(video_id.id)
    return {"message": f"Video {video_id.id} added to the queue"}

@app.get("/videos/status/")
async def videos_status():
    async with lock:
        waiting = list(video_queue._queue)
        current = processing_video
        finished = done_videos.copy()
        failed = failed_videos.copy()
    return {
        "waiting": waiting,
        "processing": current,
        "done": finished,
        "failed": failed,
    }





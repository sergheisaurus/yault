import sqlite3
import asyncio
from typing import Optional
from fastapi import FastAPI
from yt_dlp import YoutubeDL
from pydantic import BaseModel
from thumbnail import Thumbnail
from contextlib import asynccontextmanager

channel_queue = asyncio.Queue()
processing_channel: Optional[str] = None
done_channels = []
failed_channels = []

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
    channel_id: ObjectID
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
        
        if "id" in info:
            cur.execute(
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
            con.commit()

    print("Data inserted into the table")
        
    if "entries" in info:
        for entry in info["entries"]:
            if entry['title'] == info.get("title") + " - Videos":
                for video in entry['entries']:
                    cur.execute(
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
                con.commit()
        
        print(info.get("title") + " - " + str(info.get("channel_follower_count")))

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background task without awaiting it
    task = asyncio.create_task(process_channel_queue())

    yield  # Let the app run

    # Optionally cancel the task on shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.get("/channels")
async def get_all_channels():
    
    res = cur.execute("SELECT id, title, uploader_url, channel_follower_count FROM channels")
    rows = res.fetchall()
    
    if len(rows) < 1:
        return {"There are no channels available"}
    
    channels = [Channel(id=row[0], title=row[1], url=row[2], subscribers=row[3]) for row in rows]
    
    return channels

@app.get("/channel/{id}")
async def get_channel_by_id(id: str):
    channel_id = ObjectID(id=id)
    
    res = cur.execute(
        "SELECT id, title, uploader_url, channel_follower_count FROM channels WHERE id = (?)",
        (channel_id.id,)
    )
    row = res.fetchone()
    
    if row is None:
        return {f"{channel_id.id} was not found"}
    
    channel = Channel(id=row[0], title=row[1], url=row[2], subscribers=row[3])
    
    return channel

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

#TODO
@app.get("/channel/{id}/videos")
async def get_videos_by_channel(id: str):
    # return a list of all videos of a channel
    pass

@app.get("/video/{id}")
async def get_video_by_id(id: str):
    video_id = ObjectID(id=id)
    
    res = cur.execute(
        "SELECT id, channel_id, url, title, description, duration, view_count FROM videos WHERE id = (?)",
        (video_id.id,)
    )
    
    row = res.fetchone()
    
    if row is None:
        return {f"Video with id {video_id.id} was not found"}
    
    video = Video(id = row[0], channel_id=ObjectID(id=row[1]), url=row[2], title=row[3], description=row[4], duration=row[5], view_count=row[6], thumbnails=Thumbnail(row[0]).as_dict())

    return video
    
    






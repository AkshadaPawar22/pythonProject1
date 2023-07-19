import csv
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

app = FastAPI()
engine = create_engine("sqlite:///video.db", echo=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
class Video(Base):
    __tablename__ = "video"  # Use "video" as the table name
    id = Column(Integer, primary_key=True)
    title = Column(String)
    views = Column(Integer)
    likes = Column(Integer)
    dislikes = Column(Integer)
    comments = Column(Integer)

    def __repr__(self):
        return f"<Video(title='{self.title}', views={self.views}, likes={self.likes}, dislikes={self.dislikes}, " \
               f"comments={self.comments})> "

Base.metadata.create_all(bind=engine)

class VideoProfile(BaseModel):
    title: str
    views: int
    likes: int
    dislikes: int
    comments: int

def read_data():
    session = SessionLocal()
    with open('USvideos.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            video = Video(
                title=row['title'],
                views=int(row['views']),
                likes=int(row['likes']),
                dislikes=int(row['dislikes']),
                comments=int(row['comment_count'])
            )
            session.add(video)
        session.commit()
    return {'message': 'Data read and profiles created successfully'}

def get_video_count():
    session = SessionLocal()
    count = session.query(Video).count()
    return {'video_count': count}

def get_top_videos_by_views(n: int):
    session = SessionLocal()
    videos = session.query(Video).order_by(Video.views.desc()).limit(n).all()
    return {'top_videos': videos}

def get_most_liked_videos(n: int):
    session = SessionLocal()
    videos = session.query(Video).order_by(Video.likes.desc()).limit(n).all()
    return {'most_liked_videos': videos}

def get_most_disliked_videos(n: int):
    session = SessionLocal()
    videos = session.query(Video).order_by(Video.dislikes.desc()).limit(n).all()
    return {'most_disliked_videos': videos}

def get_videos_with_highest_comments(n: int):
    session = SessionLocal()
    videos = session.query(Video).order_by(Video.comments.desc()).limit(n).all()
    return {'videos_with_highest_comments': videos}

@app.get('/api/data/read')
def read_data_api():
    return read_data()

@app.get('/api/data/video_count')
def get_video_count_api():
    return get_video_count()

@app.get('/api/data/top_videos_by_views')
def get_top_videos_by_views_api(n: int):
    return get_top_videos_by_views(n)

@app.get('/api/data/most_liked_videos')
def get_most_liked_videos_api(n: int):
    return get_most_liked_videos(n)

@app.get('/api/data/most_disliked_videos')
def get_most_disliked_videos_api(n: int):
    return get_most_disliked_videos(n)

@app.get('/api/data/videos_with_highest_comments')
def get_videos_with_highest_comments_api(n: int):
    return get_videos_with_highest_comments(n)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""

import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey, Interval, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
local_timezone = timezone(timedelta(hours=1))


Base = declarative_base()

class Source(Base):
    __tablename__ = 'source'
    id = Column(String, primary_key=True)
    channel = Column(String)
    topic = Column(String)
    title = Column(String)
    description = Column(String)
    timestamp = Column(DateTime)
    duration = Column(Interval)
    size = Column(BigInteger)
    url_website = Column(String)
    url_subtitle = Column(String)
    url_video = Column(String)
    url_video_low = Column(String)
    url_video_hd = Column(String)
    filmlisteTimestamp = Column(DateTime)

class Downloaded(Base):
    __tablename__ = 'downloaded'
    did = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(local_timezone))
    source_id = Column(String, ForeignKey('source.id'))
    ignore = Column(Boolean, default=False)
    
class DataBaseManager:
    def __init__(self, configdir = "~/.config/mdl"):
        config_folder = os.path.expanduser(configdir)
        os.makedirs(config_folder, exist_ok=True)

        db_path = os.path.join(config_folder, "data.db")
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(engine)

        self.Session = sessionmaker(bind=engine)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def save_sources(self, source_data_list):
        with self.Session() as session:
            try:
                for source_data in source_data_list:
                    source_id = source_data.get('id')
                    existing_entry = None

                    if source_id:
                        existing_entry = session.query(Source).filter_by(id=source_id).first()

                    # Convert relevant columns
                    for key in ['timestamp', 'filmlisteTimestamp']:
                        source_data[key] = datetime.fromtimestamp(int(source_data[key])) if source_data.get(key) else None
                    source_data['duration'] = timedelta(seconds=source_data['duration']) if source_data.get('duration') else None


                    if existing_entry:
                        # Update existing entry
                        with session.begin_nested():
                            for key, value in source_data.items():
                                setattr(existing_entry, key, value)
                    else:
                        # Add new entry
                        source_entry = Source(**source_data)
                        with session.begin_nested():
                            session.add(source_entry)
            except Exception as e:
                print(f"Error saving sources: {e}")
                session.rollback()
        
if __name__ == "__main__":
    example_data = [{
        'channel': 'ZDF',
        'topic': 'Spielfilm-Highlights',
        'title': 'See for Me - Der unsichtbare Feind',
        'description': 'Eine durch eine Erbkrankheit erblindete Ex-Leistungssportlerin und Katzensitterin wird während eines Auftrags in einer abgelegenen Villa mit einer Einbrecherbande konfrontiert.',
        'timestamp': 1705959000,
        'duration': 5060,
        'size': 1262485504,
        'url_website': 'https://www.zdf.de/filme/spielfilm-highlights/see-for-me-der-unsichtbare-feind-102.html',
        'url_subtitle': 'https://utstreaming.zdf.de/mtt/zdf/24/01/240121_see_for_me_film_spf/5/F1034196_hoh_deu_See_for_me_240124.xml',
        'url_video': 'https://rodlzdf-a.akamaihd.net/de/zdf/24/01/240121_see_for_me_film_spf/3/240121_see_for_me_film_spf_a1a2_2360k_p35v15.mp4',
        'url_video_low': 'https://rodlzdf-a.akamaihd.net/de/zdf/24/01/240121_see_for_me_film_spf/3/240121_see_for_me_film_spf_a1a2_808k_p11v15.mp4',
        'url_video_hd': 'https://nrodlzdf-a.akamaihd.net/de/zdf/24/01/240121_see_for_me_film_spf/3/240121_see_for_me_film_spf_a1a2_3360k_p36v15.mp4',
        'filmlisteTimestamp': '1705898040',
        'id': 'LTPjRo6nFmdz9Bm7AjnyA7nlkb6fbWdZpJp17NlhK5Y='
    }]

    
    with DataBaseManager() as db_manager:
        db_manager.save_sources(example_data)
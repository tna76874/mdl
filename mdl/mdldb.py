#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
from contextlib import contextmanager
import os
import json
import re
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, ForeignKey, Interval, BigInteger, Boolean, MetaData, inspect, text, not_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, joinedload
from datetime import datetime, timedelta, timezone
from PyMovieDb import IMDB
local_timezone = timezone(timedelta(hours=1))

# import modules
import importlib
modules = [
    "mdl.thworker import *",
]

for module in modules:
    try:
        exec(f"from {module}")
    except:
        exec(f"from {module.split('.')[-1]}")

##############

Base = declarative_base()

class Meta(Base):
    __tablename__ = 'metadata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(String, ForeignKey('source.id'), unique=True)
    series = Column(String)
    season = Column(Integer)
    episode = Column(Integer)

class Source(Base):
    __tablename__ = 'source'
    id = Column(String, primary_key=True)
    imbd_id = Column(String)
    imbd_parsed = Column(Boolean, default=False)
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
    fileformat = Column(String)
    
class IMDBEntry(Base):
    __tablename__ = 'imdb'
    imdb_id = Column(String,  primary_key=True)
    typ = Column(String)
    name = Column(String)
    rating = Column(Float)
    published = Column(DateTime)
    genre = Column(String)

class Downloaded(Base):
    __tablename__ = 'downloaded'
    did = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(local_timezone))
    source_id = Column(String, ForeignKey('source.id'))
    
class DataBaseManager:
    def __init__(self, configdir = "~/.config/mdl"):
        config_folder = os.path.expanduser(configdir)
        os.makedirs(config_folder, exist_ok=True)

        db_path = os.path.join(config_folder, "data.db")
        self.engine = create_engine(f"sqlite:///{db_path}")
        
        self.ensure_all_tables()
        
        self.update_fileformat_from_url_video()

    @contextmanager
    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        try:
            yield session
        except Exception as e:
            print(f"An error occurred: {e}")
            session.rollback()
        finally:
            session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
    
    def _drop_imdb_id(self, id):
        with self.get_session() as session:
            entry = session.query(IMDBEntry).filter_by(imdb_id=id).first()
            if entry:
                session.delete(entry)
                session.commit()
        
    def _reparse_imdb_item(self, imdb_id):
        try:
            entry = self.load_json_or_use_dict(self.imdb.get_by_id(imdb_id))
            if entry.get('status', 200) == 200:
                self._add_imdb_entry(entry, source_id=None)
            else:
                if entry.get('status') == 404:
                    self._drop_imdb_id(imdb_id)

        except Exception as e:
            print(f"Error updating IMDB info for id '{imdb_id}': {e}")
            
    def _reparse_imdb_items(self):
        data = [{'imdb_id': value} for value in self._get_imdb_id_to_reparse()]
        
        if len(data)>0:
            print('Cleanup IMDB data')
            self.imdb = IMDB()
            myworker = ThreadedWorker(data, self._reparse_imdb_item)
            myworker.start_processing()

    def _get_imdb_id_to_reparse(self):
        """
        Gibt eine Liste von IMDB-IDs zurück, bei denen das Genre None ist.
        """
        with self.get_session() as session:
            ids_to_reparse = []
            entries_to_reparse = session.query(IMDBEntry).filter_by(genre=None).all()
            for entry in entries_to_reparse:
                ids_to_reparse.append(entry.imdb_id)
            return ids_to_reparse
    
    def drop_ratings_without_year(self):
        """
        Entfernt Bewertungen, bei denen das Veröffentlichungsjahr nicht angegeben ist.
        """
        with self.get_session() as session:
            entries_to_remove = session.query(IMDBEntry).filter(
                IMDBEntry.published == None
            ).all()
            for entry in entries_to_remove:
                session.delete(entry)
            session.commit()

    def get_ratings_for_imdb_ids(self, imdb_ids, year=2000):
        """
        Ruft Bewertungen für eine Liste von IMDb-IDs ab.
        
        :param imdb_ids: Eine Liste von IMDb-IDs
        :param year: Das Jahr, ab dem Einträge berücksichtigt werden sollen (Standardwert: 2000)
        :return: Ein Dictionary, das IMDb-IDs als Schlüssel und Bewertungen als Werte enthält
        """
        imdb_ids = [imdb_id for imdb_id in imdb_ids if imdb_id is not None]
        ratings = {}
        with self.get_session() as session:
            existing_entries = session.query(IMDBEntry).filter(
                IMDBEntry.imdb_id.in_(imdb_ids),
                IMDBEntry.rating.isnot(None),
                IMDBEntry.published >= datetime(year, 1, 1),
                not_(IMDBEntry.genre.like('%Documentary%')),
                not_(IMDBEntry.genre.like('%Biography%'))
            ).all()
            for entry in existing_entries:
                ratings[entry.imdb_id] = entry.rating
        return ratings

    def _add_imdb_entry(self, entry, source_id=None):
        """
        Fügt einen IMDB-Eintrag in die Datenbank ein.
        """
        with self.get_session() as session:
            entry = self.load_json_or_use_dict(entry)
            if entry:
                imdb_id_match = re.search(r'/tt(\d+)/', entry.get('url'))
                imdb_id = 'tt' + imdb_id_match.group(1) if imdb_id_match else None
                
                existing_entry = session.query(IMDBEntry).filter_by(imdb_id=imdb_id).first()
                source_data = {
                    'typ': entry.get('type'),
                    'name': entry.get('name'),
                    'rating': entry.get('rating', {}).get('ratingValue'),
                    'published': datetime.strptime(entry.get('datePublished') if entry.get('datePublished')!=None else '1970-01-01', "%Y-%m-%d"),
                    'genre': ','.join(entry.get('genre') if entry.get('genre')!=None else ['UNDEFINED'])
                }
            
                if existing_entry:
                    # Update existing entry
                    for key, value in source_data.items():
                        setattr(existing_entry, key, value)
                else:
                    # Add new entry
                    source_data['imdb_id'] = imdb_id
                    session.add(IMDBEntry(**source_data))
                
                session.commit()

                if source_id:
                    self.save_sources([{'id':source_id, 'imbd_id':imdb_id, 'imbd_parsed':True}])

    @staticmethod
    def load_json_or_use_dict(input_data):
        if isinstance(input_data, dict):
            return input_data
        try:
            return json.loads(input_data)
        except json.JSONDecodeError:
            return None
    
    def update_fileformat_from_url_video(self):
        """
        Aktualisiert das 'fileformat'-Attribut für alle Zeilen, in denen 'fileformat' NULL ist,
        indem der Wert aus 'url_video' ausgelesen wird.
        """
        with self.get_session() as session:
            # Alle Zeilen auswählen, in denen 'fileformat' NULL ist
            null_fileformat_entries = session.query(Source).filter(Source.fileformat.is_(None)).all()

            # Durch jede Zeile iterieren und 'fileformat' aktualisieren
            for entry in null_fileformat_entries:
                # 'fileformat' aus 'url_video' auslesen
                try:
                    fileformat = entry.url_video.split('.')[-1].lower()
                except:
                    continue

                entry.fileformat = fileformat
                    
            # Änderungen in die Datenbank schreiben
            session.commit()
    
    def ensure_all_tables(self):
        Base.metadata.create_all(self.engine)
        
        # Create a MetaData object
        metadata = MetaData()
    
        # Bind the MetaData object with the existing database engine
        metadata.reflect(bind=self.engine)
    
        # Iterate over all tables in the Base.metadata
        for table_name, table in Base.metadata.tables.items():
            # Get the existing table from the reflected metadata
            existing_table = metadata.tables.get(table_name)
    
            # Check if the table does not exist in the database
            if existing_table is None:
                # If the table does not exist, create it
                table.create(bind=self.engine)
    
                # Print a message indicating that the table has been created
                print(f"Table '{table_name}' created.")
            else:
                # If the table already exists, check for missing columns
                for column in table.columns:
                    # Check if the column does not exist in the existing table
                    if column.name not in existing_table.columns:
                        # If the column does not exist, add it to the existing table
                        new_column = Column(
                            column.name,
                            column.type,
                            primary_key=column.primary_key,
                            nullable=column.nullable,
                            default=column.default,
                            unique=column.unique
                        )
                        with self.engine.connect() as con:
                            add_query = f"ALTER TABLE {table_name} ADD COLUMN {new_column.compile(dialect=self.engine.dialect)}"
                            con.execute(text(add_query))
    
                        # Print a message indicating that the column has been created
                        print(f"Column '{column.name}' added to table '{table_name}'.")

    def save_sources(self, source_data_list):
        with self.get_session() as session:
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
            
            session.commit()
                
    def add_metadata(self, metadata_list):
        with self.get_session() as session:
            for metadata_data in metadata_list:
                source_id = metadata_data.get('source_id')
                existing_metadata = None

                if source_id:
                    existing_metadata = session.query(Meta).filter_by(source_id=source_id).first()

                # ensure integers
                for key in ['season', 'episode']:
                    metadata_data[key] = int(metadata_data[key]) if metadata_data.get(key) else None

                if existing_metadata:
                    with session.begin_nested():
                        for key, value in metadata_data.items():
                            setattr(existing_metadata, key, value)
                else:
                    metadata_entry = Meta(**metadata_data)
                    with session.begin_nested():
                        session.add(metadata_entry)
                
            session.commit()
                
    def get_metadata(self, source_id):
        with self.get_session() as session:
            metadata = session.query(Meta).filter_by(source_id=source_id).first()
            if metadata:
                return {
                    'id': metadata.source_id,
                    'series': metadata.series,
                    'season': metadata.season,
                    'episode': metadata.episode
                }
            else:
                return None

    def get_source_on_id(self, list_of_id, quality='M', only_not_downloaded=True, website=False, fileformat='mp4'):
        quality_column = {
            'H': 'url_video_hd',
            'M': 'url_video',
            'L': 'url_video_low',
        }

        with self.get_session() as session:
            # Basisabfrage für Quellen
            query = (
                session.query(Source)
                .filter(Source.id.in_(list_of_id))
            )
    
            if only_not_downloaded:
                subquery = (
                    session.query(Downloaded.source_id)
                    .filter(Downloaded.source_id.in_(list_of_id))
                )
                query = query.filter(~Source.id.in_(subquery))
                
            # Filtern nach dem passenden Dateiformat
            query = query.filter(Source.fileformat == fileformat)
    
            sources = query.all()

            result = []

            for source in sources:
                # Verwende die getattr-Methode, um die richtige Spalte basierend auf der Qualität zu erhalten
                link_column = quality_column.get(quality, None)

                if link_column:
                    link = getattr(source, link_column, None)
                else:
                    link = getattr(source, quality_column.get('M'), None)

                # Größe in Megabytes umrechnen
                try:
                    size_mb = source.size / (1024 * 1024)
                except:
                    size_mb = 0

                data = {
                    'id': source.id,
                    'title': source.title,
                    'link': link,
                    'duration': source.duration,
                    'timestamp': source.timestamp,
                    'size': size_mb,
                    'channel' : source.channel,
                    'format' : source.fileformat,
                    'imdb' : source.imbd_id,
                    'imdb_parsed' : source.imbd_parsed==True,
                }
                if website:
                    data['website'] = source.url_website

                result.append(data)

            return result

    def mark_as_downloaded(self, list_of_id):
        with self.get_session() as session:
            for item_id in list_of_id:
                downloaded_item = Downloaded(source_id=item_id)
                session.add(downloaded_item)

            session.commit()

    def mark_as_not_downloaded(self, list_of_id):
        with self.get_session() as session:
            for item_id in list_of_id:
                downloaded_item = session.query(Downloaded).filter_by(source_id=item_id).first()
                if downloaded_item:
                    session.delete(downloaded_item)

            session.commit()
                
    def is_downloaded(self, item_id):
        with self.get_session() as session:
            downloaded_item = (
                session.query(Downloaded)
                .filter_by(source_id=item_id)
                .first()
            )

            return downloaded_item is not None

        
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

    
    # with DataBaseManager() as db_manager:
    #     db_manager.save_sources(example_data)
    self = DataBaseManager()
    # links = self.get_source_on_id(['LiJHLZQLdDfU9V6QKWEI0zLb41nM61v0CTd6TxQ8PzM='])
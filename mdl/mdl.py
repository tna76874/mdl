#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DOWNLOAD ALL THE MOVIES
"""
import argparse
import pandas as pd
import os
import shutil
import requests
from bs4 import BeautifulSoup
import subprocess
import datetime
import re
from slugify import slugify
from urllib.parse import urlparse
from PyMovieDb import IMDB

# import modules
modules = [
    "mdl.mdldb import DataBaseManager",
    "mdl.updater import *",
    "mdl.thworker import *",
]

for module in modules:
    try:
        exec(f"from {module}")
    except:
        exec(f"from {module.split('.')[-1]}")

###############

# get version
try:
    from mdl.__init__ import __version__
except:
    from __init__ import __version__
current_version = __version__

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

class mdownloader:
    def __init__(self, **kwargs):
        self.args = {                   
                    'free': float(20),
                    'quality': 'M',
                    'series_filter': '',
                    'q': False,
                    'configdir': os.path.join(os.environ['HOME'],'.config','mdl'),
                    'download': os.path.join(os.environ['HOME'],'Downloads','Downloads_mdl'),
                    'search': 'spielfilm-highlights',
                    'series': False,
                    'channel': 'ZDF',
                    'mark_undone': False,
                    'mark_done': False,
                    'exclude': 'Audiodeskription,(ita),(swe)',
                    'min_duration': 20,
                    'title': False,
                    'run': False,
                    'file': False,
                    'index': [],
                    'imdb': None,
                    'count' : 20000,
                    'threads': 10,
                    'year' : 2000,
                    'imdb_reset' : False,
                    'nfo' : False,
                    }
        self.args.update(kwargs)
        self.args['series_filter'] = [k.strip() for k in self.args['series_filter'].split(';')]

        if self.args['q']: self.args['download'] = os.getcwd()
        
        for i in ['configdir', 'download']:
            self.args[i]  = os.path.abspath(self.args[i])
        
        self.args['logfile']  = os.path.join(self.args['configdir'],"processed.log")
        self.args['baseurl']  = "https://mediathekviewweb.de/feed?query="
        
        self._reset_dataframe()
        
        self.db = DataBaseManager(configdir=self.args['configdir'])
        
        self.print = ['title', 'channel']
        
        if os.path.exists(self.args['logfile']) & ~self.args['q']:
            with open(self.args['logfile']) as f:
                processed = [line.strip() for line in f.readlines()]
                
            self.db.mark_as_downloaded(processed)
                
            os.remove(self.args['logfile'])

        if (self.args['imdb_reset'] == True): self.db._reset_imdb()
        
        if self.args['imdb']!=None: self.args['search']='Spielfilm,Kino Film,Filme im Ersten,Filme'

        if (self.args['search'] != None) & (not self.args['series']): self.get_info()
        
        if self.args['mark_done']: self.mark_as_done()

        if self.args['mark_undone']: self.mark_as_undone()
          
        if self.args['run'] & (not self.args['series']): self.download_movies()

        if self.args['series']: self.series_downloader()

    def _reset_dataframe(self):
        self.DF_links = pd.DataFrame()
        
    def processed(self, itemid):
        return self.db.is_downloaded(itemid)
    
    def get_series_metadata_from_id(self, id_list):
        DF_links = pd.DataFrame(self.db.get_source_on_id(id_list, only_not_downloaded=False, website=True))
        for _, row in DF_links.iterrows():
            # URL der Webseite, die geparst werden soll
            url = row['website']
            
            if url.startswith("https://www.zdf.de/serien"):
                # HTML-Code von der URL abrufen
                response = requests.get(url)
                html_code = response.text
                
                try:
                    # BeautifulSoup-Objekt erstellen
                    soup = BeautifulSoup(html_code, 'html.parser')
                    
                    # Text aus dem span-Tag extrahieren und aufteilen
                    teaser_text = soup.find('span', class_='teaser-cat').get_text(strip=True)
                    
                    # Verwenden von regulären Ausdrücken, um nach dem Muster "Staffel X, Folge Y" zu suchen
                    match = re.search(r'Staffel (\d+),\s*Folge (\d+)', teaser_text)
            
                    if match:
                        season = int(match.group(1))
                        episode = int(match.group(2))
    
                        self.db.add_metadata([{'source_id': row['id'], 'season': season, 'episode': episode}])
                except:
                    pass

    def _parse_movie_info(self, movie):
        # Regular Expression, um den Titel, das Land und das Jahr zu extrahieren
        pattern = r"^(.*?) - Spielfilm, (.*?)(?: (\d{4}))(?: .*)?$"
        match = re.match(pattern, movie)
        
        if match:
            title = match.group(1)
            country = match.group(2)
            year = match.group(3)
            return {"title": title, "land": country, "year": year}
        else:
            return {}

    def _parse_film_info_from_description(self, multi_line_string):
        pattern = r"Spielfilm (\w+) (\d{4})"
        lines = multi_line_string.split('\n')
        parsed_info = {}
        for line in lines:
            match = re.match(pattern, line.strip())
            if match:
                land = match.group(1)
                jahr = match.group(2)
                parsed_info["land"] = land
                parsed_info["year"] = jahr
                break
        return parsed_info
    
    def _apply_parse_movie_info(self, df):
        # Filtern der Zeilen, in denen 'p_title' None ist
        df['p_title'] = None
        none_mask = df['p_title'].isna()
        
        # Kopieren des ursprünglichen DataFrames, um Änderungen vorzunehmen
        modified_df = df.copy()
        
        try:
            ## ALL TAGGED SPIELFILME
            # Übernahme von 'p_title', 'p_land' und 'p_year' aus 'title', wo 'p_title' None ist
            parsed_info = modified_df.loc[none_mask, 'title'].apply(self._parse_movie_info)
            parsed_df = pd.DataFrame(parsed_info.tolist(), index=parsed_info.index)
            modified_df.loc[none_mask, ['p_title', 'p_land', 'p_year']] = parsed_df[['title', 'land', 'year']].values
        except:
            pass
                
        try:
            # Filtern der Zeilen, in denen 'topic' den Wert 'Filme im Ersten' hat
            first_channel_mask = modified_df['topic'] == 'Filme im Ersten'
            ### ARD FILME IM ERSTEN
            # Parsed Informationen von 'description' mit '_parse_film_info_from_description' für 'p_land' und 'p_year'
            parsed_info = modified_df.loc[first_channel_mask, 'description'].apply(self._parse_film_info_from_description)
            parsed_df = pd.DataFrame(parsed_info.tolist(), index=parsed_info.index)
        
            # Hinzufügen der neuen Spalten 'p_land' und 'p_year' zum modifizierten DataFrame für 'Filme im Ersten'
            modified_df.loc[first_channel_mask, ['p_land', 'p_year']] = parsed_df[['land', 'year']].values
            modified_df.loc[first_channel_mask, 'p_title'] = modified_df.loc[first_channel_mask, 'title']
        except:
            pass
        
        try:
            ### ARD FILME
            # Übernahme von 'p_title' aus 'title', wo 'channel' den Wert 'ARD' und 'topic' den Wert 'Filme' hat
            ard_filme_mask = (modified_df['channel'] == 'ARD') & (modified_df['topic'] == 'Filme')
            modified_df.loc[ard_filme_mask, 'p_title'] = modified_df.loc[ard_filme_mask, 'title']       
        except:
            pass
        
        return modified_df
    
    def get_links(self):
        self._reset_dataframe()

        QUERIES = [{'fields': ['title', 'topic'],'query': k} for k in self.args['search'].split(',')]
        if self.args['channel'].split(',') != ['']: QUERIES += [{'fields': ['channel'],'query': k} for k in self.args['channel'].split(',')]
        DF_links = pd.DataFrame()
        skip = 0
        while True:
            data =      {  
                        'queries': QUERIES,
                        'sortBy': 'timestamp',
                        'sortOrder': 'desc',
                        'future': 'true',
                        'offset': skip,
                        'size': 50,
                        'duration_min': 20,
                        'duration_max': 10000,
                        }
            try:
                DF_tmp = pd.DataFrame(requests.post('https://mediathekviewweb.de/api/query',  json=data, headers={'content-type': 'text/plain'}).json()['result']['results'])
                DF_links = pd.concat([DF_links, DF_tmp], ignore_index=True)
                skip+=50
                if len(DF_tmp)==0: break
            except:
                break
        
        self.db.save_sources(DF_links.to_dict(orient='records'))
        
        try:
            DF_links = pd.DataFrame(self.db.get_source_on_id(DF_links['id'].values, only_not_downloaded=(self.args['q']==False) and (not self.args['mark_undone']), quality=self.args['quality']))
        except:
            DF_links = pd.DataFrame()

        if not DF_links.empty:    
            #exclude useless sources
            excluded_tags = [
                            'Audiodeskription',
                            '(ita)',
                            '(Englisch)',
                            '(Französisch)',
                            '(dan)',
                            'Hörfassung',
                            '(Englische Originalfassung)',
                            '(Originalversion)',
                            'Originalversion',
                            'Originalfassung',
                            '(OV)',
                            ]
            for i in list(set(self.args['exclude'].split(',')) | set(excluded_tags)):
                DF_links = DF_links[(~DF_links['title'].str.contains(i, regex=False))]
            
            # cleanup titles
            DF_links['title'] = DF_links['title'].str.replace("/",' ')  
            
            # dropping prewiew sources
            DF_links = DF_links[DF_links['duration']>datetime.timedelta(minutes=self.args['min_duration'])].reset_index(drop=True)
            
            # sort after publish date
            DF_links.sort_values('timestamp',inplace=True)
            
            if self.args['imdb']!=None:
                DF_links = self._apply_parse_movie_info(DF_links)
                DF_links = DF_links.dropna(subset=['p_title'])
                self._update_imdb_info(DF_links)
                ratings = self.db.get_ratings_for_imdb_ids(DF_links['imdb'].values, year=self.args['year'])
                DF_links['rating'] = DF_links['imdb'].map(lambda k: ratings.get(k,{}).get('rating'))
                DF_links['ratingCount'] = DF_links['imdb'].map(lambda k: ratings.get(k,{}).get('ratingCount'))
                DF_links['year'] = DF_links['p_year'].apply(lambda x: str(int(x)) if pd.notna(x) else '---')
                DF_links = DF_links[(DF_links['rating']>=self.args['imdb']) & (DF_links['ratingCount']>=self.args['count'])]
                DF_links = DF_links.sort_values(by=['rating', 'p_year', 'title'], ascending=[False, False, True])
                DF_links = DF_links.drop_duplicates(subset='imdb', keep='first')
                
                self.print.extend(['rating', 'year', 'imdb'])
            
            if self.args['title']: DF_links['title'] = DF_links['title'].apply(lambda x: x.split(' - ')[0])
            
            self.DF_links = DF_links.reset_index(drop=True)
            
            if self.args['index']!=[]:
                self.DF_links = self.DF_links[self.DF_links.index.isin(self.args['index'])]
    
    def _update_imdb_info(self, DF_links):
        self.db._reparse_imdb_items()
        
        DF_imdb = DF_links[DF_links['imdb_parsed']==False][['id','p_title', 'p_year']]
        DF_imdb = DF_imdb.rename(columns={'id':'source_id','p_title':'title','p_year':'year',})
        
        data = DF_imdb[~pd.isnull(DF_imdb['year'])].to_dict(orient='records')
        if len(data)>0:
            myworker = ThreadedWorker(data, self.db._update_imdb_info_entry, info='Getting metadata from IMDB (TAGGED)')
            myworker.start_processing()

        data = DF_imdb[pd.isnull(DF_imdb['year'])].to_dict(orient='records')
        if len(data)>0:
            myworker = ThreadedWorker(data, self.db._update_imdb_info_entry, info='Getting metadata from IMDB (UNTAGGED)')
            myworker.start_processing()
            
    def _get_download_filename_from_url(self, URL):
            parsed_url = urlparse(URL)
            path_components = parsed_url.path.split('/')
            try:
                return path_components[-1] if path_components[-1] else None
            except:
                return None

    def ensure_dir(self,DIR):
        dirlist = os.path.normpath(DIR).split(os.sep)
        for i in range(len(dirlist)):
            tmpdir = os.path.abspath(os.sep.join(dirlist[:i+1]))
            if not os.path.exists(tmpdir):
                os.mkdir(tmpdir)
                print('Create {:}'.format(tmpdir))
    
    def get_info(self):
        self.get_links()
        if not self.DF_links.empty:
            print(self.DF_links[self.print])
            print("Download {:d} movies ({:.1f}GB)".format(len(self.DF_links),float(self.DF_links['size'].sum()/1024)))
        else:
            print("No sources found!")

    def mark_as_done(self):
        self.db.mark_as_downloaded(self.DF_links['id'].values)

    def mark_as_undone(self):
        self.db.mark_as_not_downloaded(self.DF_links['id'].values)

    def check_free_space(self):
        """
        return free disk space in GB
        """
        self.ensure_dir(self.args['download'])
        disk = os.statvfs(self.args['download']+'/')
        return float(disk.f_bsize*disk.f_bfree)/1024/1024/1024
    
    def _wget(self, FILENAME, URL):
        try:
            CMD=["wget", "--timeout=3" ,"-c" ,"-O", FILENAME, URL]
            print(f"Start downloading: {FILENAME}")
            result = subprocess.run(CMD,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT)
            
            return result.returncode==0
        except:
            return False

    def wget(self,row,TITLE):
        """
        download URL
        """
        URL = row['link']
        if self.args['file']:
            DOWNLOAD_FILENAME = f'{TITLE}.mp4'
            DOWNLOAD_BASEDIR = self.args['download']
        else:
            URL_FILENAME = self._get_download_filename_from_url(URL)
            DOWNLOAD_BASEDIR = os.path.join(self.args['download'], row['title'])
            if URL_FILENAME:
                DOWNLOAD_FILENAME = URL_FILENAME
            else:
                DOWNLOAD_FILENAME = f'{TITLE}.mp4'
        
        # setting paths
        DOWNLOAD_PATH_FILENAME = os.path.join(DOWNLOAD_BASEDIR, DOWNLOAD_FILENAME)
        PARTIAL_FILENAME = f'{DOWNLOAD_PATH_FILENAME}.partial'
        
        # ensure directory exists
        self.ensure_dir(os.path.dirname(DOWNLOAD_PATH_FILENAME))
        
        # try downloading file
        max_attempts, attempt, success = 3, 0, False
        while attempt < max_attempts and not success:
            success = self._wget(PARTIAL_FILENAME, URL)
            attempt += 1
            
        # cleanup
        if os.path.exists(PARTIAL_FILENAME):
            if success:
                shutil.move(PARTIAL_FILENAME, DOWNLOAD_PATH_FILENAME)
            else:
                os.remove(PARTIAL_FILENAME)
                
        return success, DOWNLOAD_BASEDIR, DOWNLOAD_FILENAME

    def download_movies(self):
        for i, row in self.DF_links.iterrows():
            self.get_series_metadata_from_id([row['id']])
            meta = self.db.get_metadata(row['id'])
            
            TITLE = slugify(row['title'], separator='_', lowercase=False)
            if meta:
                TITLE = os.path.join(f'Staffel {meta["season"]:d}',f'S{meta["season"]:02d}E{meta["episode"]:02d}_{TITLE}')

            if (self.check_free_space() - self.DF_links.loc[i,'size'] / 1024) > float(self.args['free']):
                is_downloaded, DOWNLOAD_BASEDIR, DOWNLOAD_FILENAME = self.wget(row, TITLE)

                if not self.args['q'] and is_downloaded:
                    self.db.mark_as_downloaded([self.DF_links.loc[i,'id']])
                    if (self.args['imdb']!=None) or (self.args['nfo']==True):
                        nfo_filename = DOWNLOAD_FILENAME.replace('.mp4','')
                        self.create_movie_nfo(row, DOWNLOAD_BASEDIR, filename=nfo_filename)
            else:
                print("No free disk space. Skip download.")
                
    def create_movie_nfo(self, metadata, download_path, filename='movie'):
        nfo_data = {
            'title': metadata.get('p_title') or metadata.get('title') or '',
            'plot': metadata.get('description') or '',
            'year': metadata.get('p_year') or '',
            'imdbid': metadata.get('imdb') or '',
            'country': metadata.get('p_land') or '',
            'dateadded': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    
        nfo_content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        nfo_content += '<movie>\n'
        for tag, value in nfo_data.items():
            nfo_content += f'    <{tag}>{value}</{tag}>\n'
        nfo_content += '</movie>'
    
        nfo_file_path = os.path.join(download_path, f'{filename}.nfo')
    
        with open(nfo_file_path, 'w', encoding='utf-8') as nfo_file:
            nfo_file.write(nfo_content)
                
    def series_downloader(self):
        url = "https://www.zdf.de/serien"
        response = requests.get(url)
        
        download_base_dir = self.args['download']
        
        soup = BeautifulSoup(response.text, 'html.parser')
                
        b_cluster_posters = soup.find_all('article', class_='b-cluster-poster')
        b_clusters = soup.find_all('article', class_='b-cluster')
        
        combined_sections = b_cluster_posters + b_clusters
        
        series = dict()
        
        # Iteriere über alle Abschnitte in der kombinierten Liste
        for section in combined_sections:
            # Dein Code, um Informationen aus den Abschnitten zu extrahieren
            h2_element = section.find('h2', class_='cluster-title')
            # Weitere Extraktionen oder Aktionen...
            if h2_element:
                h2_title = h2_element.text.strip()
                serieslist = list()
                a_elements = section.find_all('a', class_='teaser-title-link')
                for a_element in a_elements:
                    serieslist.append(a_element.get('title'))
                series[h2_title] = serieslist
        
        serien = list(set([value.strip() for key in self.args['series_filter'] if key in series for value in series[key]]))
        serien.sort()

        print(pd.DataFrame({'title': serien}))
        
        self.args['title']=True
        self.args['file']=True
        
        if self.args['channel']=='':
            self.args['channel']='ZDF'

        if self.args['run']:
            for serie_name in serien:
                serie_name_folder = slugify(serie_name, lowercase=False, separator=' ', replacements=[["'",""]], allow_unicode=True)
                self.args['download'] = os.path.join(download_base_dir, serie_name_folder)
                self.args['search'] = serie_name
                self.get_info()
                self.download_movies()


def main(headless=True):
    parser = argparse.ArgumentParser(
        epilog=f"version {current_version}"
    )

    parser.add_argument("--configdir", help="directory where config is stored", default=os.path.join(os.environ['HOME'],'.config','mdl'),type=str)
    parser.add_argument("--download", help="directory where downloads are stored", default=os.path.join(os.environ['HOME'],'Downloads','Downloads_mdl'),type=str)
    parser.add_argument("--search", help="Comma seperated search keywords", default="spielfilm-highlights",type=str)
    parser.add_argument("--quality", help="Set quality: high (H), medium (M), low (L)", default="M",type=str, choices=["H", "M", "L"])
    parser.add_argument("--channel", help="Comma seperated channel keywords", default="",type=str)
    parser.add_argument("--exclude", help="Comma seperated exclude keywords", default="Audiodeskription,(ita),(swe)",type=str)
    parser.add_argument("--min-duration", help="Minimum duration in minutes", default=20,type=int)
    parser.add_argument("--free", help="Minimum free disk space (GB)", default=20,type=float)
    parser.add_argument("-q", help="Quick mode: Do not memorize downloaded content and download to current directory", action="store_true")
    parser.add_argument("--file", help="Do not create directory for each source", action="store_true")
    parser.add_argument("--run", help="run downloads", action="store_true")
    parser.add_argument("--title", help="Cut unneccessary parts from title", action="store_true")
    parser.add_argument("--mark-done", help="Mark found IDs as done.", action="store_true")
    parser.add_argument("--mark-undone", help="Mark found IDs as undone.", action="store_true")
    parser.add_argument("--series", help="Automatic series downloader (zdf.de/serien) of series.", action="store_true")
    parser.add_argument("--series-filter", help="; (not comma) seperated series topics: e.g. Top-Serien zum Streamen;Drama-Serien", default='Top-Serien zum Streamen;Drama-Serien;Thriller-Serien;Comedy-Serien;Internationale Serien;neoriginal;Beliebte Serien;Krimi-Serien',type=str)
    parser.add_argument("--index", help="Additional search parameter to select sources", nargs='+', type=int, default=[])
    parser.add_argument("--imdb", help="IMDB rating filter", type=float)
    parser.add_argument("--imdb-reset", help="IMDB reset", action="store_true")
    parser.add_argument("--nfo", help="create movies.nfo in download folder", action="store_true")
    parser.add_argument("--year", help="Minimum year for IMDB rating filter", type=int, default=2000)
    parser.add_argument("--version",  action="store_true", help=f"show version")
    parser.add_argument("--upgrade",  action="store_true", help=f"ensure latest version")


    args = parser.parse_args()

    if args.version:
        print(f"{current_version}")
        exit()
        
    if args.upgrade:
        VersionCheck().ensure_latest_version()
        exit()
  
    # init object
    if headless: _ = mdownloader(**vars(args))
    else: return mdownloader(**vars(args))

if __name__ == "__main__":   
    self = main(headless=False)
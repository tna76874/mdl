#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DOWNLOAD ALL THE MOVIES
"""
import argparse
import pandas as pd
import os
import requests
import subprocess
import datetime
import re

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

class mdownloader:
    def __init__(self, **kwargs):
        self.args = dict()
        self.args.update(kwargs)
        
        if self.args['q']: self.args['download'] = os.getcwd()
        
        for i in ['configdir', 'download']:
            self.args[i]  = os.path.abspath(self.args[i])
        
        self.args['logfile']  = os.path.join(self.args['configdir'],"processed.log")
        self.args['baseurl']  = "https://mediathekviewweb.de/feed?query="
        
        self.DF_links = pd.DataFrame()
        
        if os.path.exists(self.args['logfile']) & ~self.args['q']:
            with open(self.args['logfile']) as f:
                self.processed = f.readlines()
        else:
            self.processed=[]
           
        if self.args['search'] != None: self.get_info()
          
        if self.args['run']: self.download_movies()
        
    
    def get_links(self):
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
            DF_tmp = pd.DataFrame(requests.post('https://mediathekviewweb.de/api/query',  json=data, headers={'content-type': 'text/plain'}).json()['result']['results'])
            DF_links = DF_links.append(DF_tmp, ignore_index=True)
            skip+=50
            if len(DF_tmp)==0: break

        if not DF_links.empty:
            # Converting size in MB
            DF_links['size'] = DF_links['size'] / (1024*1024)
            
            # Converting timestamps
            DF_links['timestamp'] = DF_links['timestamp'].apply(lambda x: pd.to_datetime(datetime.datetime.utcfromtimestamp(int(x))))
            
            # renaming columns
            DF_links.rename(columns={'url_video_hd': 'link'}, inplace=True)
            
            # dropping useles columns
            DF_links = DF_links[['id','title','link','duration','timestamp','size']]
            
            #exclude useless sources
            for i in list(set(self.args['exclude'].split(',')) | set(['Audiodeskription', '(ita)'])):
                DF_links = DF_links[(~DF_links['title'].str.contains(i, regex=False))]
            
            # formatting id columns
            DF_links['id'] = DF_links['id'] + '\n'
            
            # exclude all processed sources
            DF_links = DF_links[~DF_links['id'].isin(self.processed)]
            
            # cleanup titles
            DF_links['title'] = DF_links['title'].str.replace("/",' ')  
            
            # dropping prewiew sources
            DF_links = DF_links[DF_links['duration'].astype(int)>(self.args['min_duration']*60)].reset_index(drop=True)
            
            
            # sort after publish date
            DF_links.sort_values('timestamp',inplace=True)
                
            self.DF_links = DF_links.reset_index(drop=True)
        
    def ensure_dir(self,DIR):
        dirlist = os.path.normpath(DIR).split(os.sep)
        for i in range(len(dirlist)):
            tmpdir = os.path.abspath(os.sep.join(dirlist[:i+1]))
            if not os.path.exists(tmpdir):
                os.mkdir(tmpdir)
                print('Create {:}'.format(tmpdir))
    
    def wget(self,URL,TITLE):
        """
        download URL
        """
        if self.args['file']:
            self.ensure_dir(self.args['download'])
            FILENAME=os.path.join(self.args['download'],re.sub(r'[\\/*?:"<>|& ]',"_",TITLE+'.mp4'))
            result = subprocess.run(["wget", "-c" ,"-O", FILENAME, URL],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT)
        else:
            DIR=os.path.join(self.args['download'],TITLE)
            self.ensure_dir(DIR)
            result = subprocess.run(["wget", "-c" ,"-P", DIR, URL],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT)

    def get_info(self):
        self.get_links()
        if not self.DF_links.empty:
            print(self.DF_links[['title']])
            print("Download {:d} movies ({:.1f}GB)".format(len(self.DF_links),float(self.DF_links['size'].sum()/1024)))
        else:
            print("No sources found!")
        
    def download_movies(self):
        if ~isinstance(self.DF_links, pd.DataFrame): 
            self.get_info()

        for i in self.DF_links.index:
            
            print("Start downloading: {:}".format(self.DF_links.loc[i,'title']))

            self.wget(self.DF_links.loc[i,'link'],self.DF_links.loc[i,'title'])
            
            if not self.args['q']:
                self.ensure_dir(self.args['configdir'])
                with open(self.args['logfile'], "a") as file:
                    file.write(self.DF_links.loc[i,'id'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configdir", help="directory where config is stored", default=os.path.join(os.environ['HOME'],'.config','mdl'),type=str)
    parser.add_argument("--download", help="directory where downloads are stored", default=os.path.join(os.environ['HOME'],'Downloads','Downloads_mdl'),type=str)
    parser.add_argument("--search", help="Comma seperated search keywords", default="spielfilm-highlights",type=str)
    parser.add_argument("--channel", help="Comma seperated channel keywords", default="",type=str)
    parser.add_argument("--exclude", help="Comma seperated exclude keywords", default="Audiodeskription,(ita)",type=str)
    parser.add_argument("--min-duration", help="Minimum duration in minutes", default=10,type=int)
    parser.add_argument("-q", help="Quick mode: Do not memorize downloaded content and download to current directory", action="store_true")
    parser.add_argument("--file", help="Do not create directory for each source", action="store_true")
    parser.add_argument("--run", help="run downloads", action="store_true")

    args = parser.parse_args()

    # init object
    mdl = mdownloader(**vars(args))
    return mdl


if __name__ == '__main__':
    mdl = main()
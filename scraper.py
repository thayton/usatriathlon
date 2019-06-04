import os
import csv
import time
import json
import logging
import pathlib
import datetime
import argparse
import requests

from redis import StrictRedis
from redis.exceptions import RedisError
from rediscache import RedisCache
from bs4 import BeautifulSoup

# (year, race_type_id, country_id, state_id)
#   - event_id
#     - race_id
#     - race_id
#   - event_id
#     - race_id
#     - race_id

# Cache event list using (year, race_type_id, country_id, state_id) as key
# Cache list of races using event_id as key
# Cache results of race using race_id as key

class UsaTriathlonScraper(object):
    def __init__(self):
        self.url = 'https://rankings.usatriathlon.org/Event/Events'
        self.session = requests.Session()

        FORMAT = "%(asctime)s [ %(filename)s:%(lineno)s - %(funcName)s() ] %(message)s"
        logging.basicConfig(format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        self.cache = None
        self.init_cache()

    def init_cache(self):
        redis_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': 'foobared'
        }

        client = StrictRedis(**redis_config)
        try:
            client.ping()
        except RedisError as ex:
            exit(f'Failed to connect to Redis - {ex}, exiting...' )

        self.cache = RedisCache(client=client)
        
    def event_filename(self, year, state, event):
        return f'results/{year}/{state["CountryId"]}/{state["StateCode"]}/{event["EventId"]}/event.csv'        

    def race_data_filename(self, year, race_type, state, event, race_id):
        return f'results/{year}/{state["CountryId"]}/{state["StateCode"]}/{event["EventId"]}/{race_type}/{race_id}/race_data.csv'

    def race_results_filename(self, year, race_type, state, event, race_id):
        return f'results/{year}/{state["CountryId"]}/{state["StateCode"]}/{event["EventId"]}/{race_type}/{race_id}/race_results.csv'

    def csv_save(self, filename, data, headers):
        '''
        Save data (which is expected to be a list) to filename as csv using headers
        '''
        # Create the path to filename if it does not yet exist...
        path = pathlib.Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(f'{filename}', 'w') as fp:
            writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(headers)

            for d in data:
                row = [ d.get(k, '') for k in headers ]
                writer.writerow(row)
        
    def get_event_list(self, year, race_type_id, country_id, state_id):
        '''
        Search for a list of event IDs for the given year, race_type, country, state
        '''
        url = 'https://rankings.usatriathlon.org/Event/List'
        data = {
            'Year': year,
            'ShowSanctioned': None,
            'RaceTypeId': race_type_id,
            'CountryId': country_id,
            'StateId': state_id,
            'SearchCriteria': ''
        }

        resp = self.session.post(url, json=data)
        json_events = resp.json()

        return json_events

    def get_races_at_event(self, event_id):
        '''
        Return list of races that took place at event
        '''
        race_ids = []

        #
        # Open the page for the event and scrape the list of URLs
        # for the races at this event and extract the race IDs
        #
        # Event URLs consist of event_id followed by race_type_id. So,
        #
        #   https://rankings.usatriathlon.org/Event/ViewEvent/301597/2
        #
        # corresponds to event_id 301597 and race_type_id 2 (duathlon)
        #
        url = f'https://rankings.usatriathlon.org/Event/ViewEvent/{event_id}'
        
        try:
            html = self.cache[url]
        except KeyError:
            time.sleep(1.5)
            
            resp = self.session.get(url)                
            html = resp.text
            
            self.cache[url] = html

        soup = BeautifulSoup(html, 'html.parser')

        for li in soup.select('ul#racesList > li.raceLink'):
            race_ids.append(li['raceid'])

        return race_ids
    
    def get_race_data(self, race_id):
        url = 'https://rankings.usatriathlon.org/Race/GetRaceData/'
        data = {
            'RaceId': race_id
        }
        
        key = f'https://rankings.usatriathlon.org/Race/GetRaceData/{race_id}'
        try:
            text = self.cache[key]
        except KeyError:
            time.sleep(1.5)
            
            resp = self.session.post(url, json=data)
            text = resp.text
            
            self.cache[key] = text
        
        json_results = json.loads(text)
        return json_results['Race']
    
    def get_race_results(self, race_id):
        '''
        Return the results table for a given race
        '''
        url = 'https://rankings.usatriathlon.org/RaceResult/GetResults/'
        data = {
            'RaceId': race_id,
            'DivisionId': '',
            'Gender': '',
            'Category': '',
            'AllowCaching': False,
            'FirstName': ''
        }

        key = f'https://rankings.usatriathlon.org/RaceResult/GetResults/{race_id}'
        try:
            text = self.cache[key]
        except KeyError:
            time.sleep(1.5)
                        
            resp = self.session.post(url, json=data)
            text = resp.text
            
            self.cache[key] = text
        
        json_results = json.loads(text)

        if json_results['Results'] is not None:
            race_results = json.loads(json_results['Results'])
        else:
            race_results = []
            
        return race_results
    
    def get_dropdown_options(self):
        opts = {}
        resp = self.session.get(self.url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        inputs = (
            # id                 opts-key
            ('YearsSource',     'years'),
            ('RaceTypesSource', 'race_types'),
            ('StatesSource',    'states')
        )

        for input_id, name in inputs:
            inp = soup.find(id=input_id)
            opts[name] = json.loads(inp['value'])

        return opts

    def search_opts(self, year_filter=None):
        opts = self.get_dropdown_options()
        
        if year_filter:
            years = [ year_filter ]
        else:
            years = opts['years']
            
        for year in years:
            for race_type in opts['race_types']:
                for state in opts['states']:
                    self.logger.debug(f"{year}-{race_type['Value']}-{state['CountryId']}-{state['StateName']}")
                    yield ( year, race_type, state )
        
    def scrape(self, year_filter=None):
        for year, race_type, state in self.search_opts(year_filter):
            events = self.get_event_list(
                year,
                race_type['RaceTypeId'],
                state['CountryId'],
                state['StateId']
            )

            # Note that the race_type_id we use in the search becomes irrelevant later
            # as we take list of all races (and race_types) from the event page. In other
            # words, even though we search for Triathlon events, all type of events will
            # be listed on the event page.
            for e in events:
                event_file = self.event_filename(year, state, e)
                self.csv_save(event_file, [e], e.keys())

                self.logger.debug(f'Getting races at event {e["EventId"]}')                
                races = self.get_races_at_event(e['EventId'])
                
                for race_id in races:
                    self.logger.debug(f'Getting race data for {race_id} at event {e["EventId"]}')
                    race_data = self.get_race_data(race_id)
                    race_data_file = self.race_data_filename(year, race_data['RaceType'], state, e, race_id)
                    self.csv_save(race_data_file, [race_data], race_data.keys())

                    # if race_data['ResultsType'] is set then there are results...
                    if race_data['ResultsType'] == '':
                        continue
                    
                    self.logger.debug(f'Getting race results for {race_id} at event {e["EventId"]}')
                    race_results = self.get_race_results(race_id)
                    race_results_file = self.race_results_filename(year, race_data['RaceType'], state, e, race_id)
                    if len(race_results) > 0:
                        self.csv_save(race_results_file, race_results, race_results[0].keys())

                # Mark event as complete so we don't hit it again in separate search...
                
if __name__ == '__main__':
    current_year = int(datetime.datetime.now().year)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", type=int, help="Limit search to specific year", choices=range(current_year, 2008, -1))

    args = parser.parse_args()

    scraper = UsaTriathlonScraper()
    scraper.scrape(args.year)

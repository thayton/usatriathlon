import os
import csv
import json
import pathlib
import requests

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
        Return a list of event IDs for the given year,race_type,state
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

        # XXX Cache json results string using key like
        # https://rankings.usatriathlon.org/Event/List/{year}/{race_type_id}/{country_id}/{state_id}/
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

        # XXX Cache html results for 'https://rankings.usatriathlon.org/Event/ViewEvent/{event_id}'
        resp = self.session.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        for li in soup.select('ul#racesList > li.raceLink'):
            race_ids.append(li['raceid'])

        return race_ids
    
    def get_race_data(self, race_id):
        url = 'https://rankings.usatriathlon.org/Race/GetRaceData/'
        data = {
            'RaceId': race_id
        }
        
        # XXX Cache json_results string using key like
        # https://rankings.usatriathlon.org/Race/GetRaceData/{race_id}
        resp = self.session.post(url, json=data)
        json_results = resp.json()

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

        # XXX Cache json_results string using key like
        # https://rankings.usatriathlon.org/RaceResult/GetResults/{race_id}
        resp = self.session.post(url, json=data)
        json_results = resp.json()
        race_results = json.loads(json_results['Results'])
        
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

    def search_opts(self):
        opts = self.get_dropdown_options()
        for year in opts['years']:
            for race_type in opts['race_types']:
                for state in opts['states']:
                    print(f"{year}-{race_type['Value']}-{state['CountryId']}-{state['StateName']}")
                    yield ( year, race_type, state )
        
    def scrape(self):
        for year, race_type, state in self.search_opts():
            events = self.get_event_list(
                year,
                race_type['RaceTypeId'],
                state['CountryId'],
                state['StateId']
            )
            print(json.dumps(events, indent=2))
            break

        # Note that the race_type_id we use in the search becomes irrelevant later
        # as we take list of all races (and race_types) from the event page. In other
        # words, even though we search for Triathlon events, all type of events will
        # be listed on the event page.
        # XXX Are event_ids unique across years?
        for e in events:
            # Save event information
            event_id = e['EventId']            
            event_file = self.event_filename(year, state, e)
            self.csv_save(event_file, [e], e.keys())
            
            event_id = '249854'
            
            races = self.get_races_at_event(event_id)
            for race_id in races:
                race_data = self.get_race_data(race_id)
                race_data_file = self.race_data_filename(year, race_data['RaceType'], state, e, race_id)
                self.csv_save(race_data_file, [race_data], race_data.keys())
                
                # if race_data['ResultsType'] is set then there are results...
                race_results = self.get_race_results(race_id)
                race_results_file = self.race_results_filename(year, race_data['RaceType'], state, e, race_id)                
                
                self.csv_save(race_results_file, race_results, race_results[0].keys())
                
if __name__ == '__main__':
    scraper = UsaTriathlonScraper()
    scraper.scrape()

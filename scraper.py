import json
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

        return [ e['EventId'] for e in json_events ]

    def get_races_at_event(self, event_id):
        '''
        Return list of races that took place at event
        '''
        race_ids = []
        
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

        return json.loads(json_results['Results'])
    
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
                    yield (
                        year,
                        race_type['RaceTypeId'],
                        state['CountryId'],
                        state['StateId']
                    )
        
    def scrape(self):
        for year, race_type_id, country_id, state_id in self.search_opts():
            events = self.get_event_list(
                year, race_type_id, country_id, state_id
            )
            print(json.dumps(events, indent=2))
            break

        # XXX Are event_ids unique across years?
        for e in events:
            e = '249854'
            races = self.get_races_at_event(e)
            for race_id in races:
                race_data = self.get_race_data(race_id)
                # if race_data['ResultsType'] is set then there are results...
                race_results = self.get_race_results(race_id)
                
if __name__ == '__main__':
    scraper = UsaTriathlonScraper()
    scraper.scrape()

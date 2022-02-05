import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
from tqdm import tqdm
from data_utils import *


class UnderStat:
    def __init__(self, load_exists=True, save=True):
        self.seasons = ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
        self.leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1']
        self.matches  = list(range(1, 39))
        self.dates_config =  self._get_dates_config(load_exists=True, save=True)


    def _get_dates_config(self, load_exists=True, save=True):
        if load_exists:
            try:
                configs = json.load(open('football_dates_config.json', 'r'))
                return configs
            except Exception as err:
                print("Failed to load configs, Fetching configs instead ...")
                return self.scrape_dates(save)
        return self.scrape_dates(save)

    def scrape_dates(self, save=True):

        league_to_code = {
            'La_liga': 'ES1',
            'Bundesliga': 'L1',
            'EPL': 'GB1',
            'Serie_A': 'IT1',
            'Ligue_1': 'FR1'
        }
        fetch_configs = {
            l: {s: {"match_" + str(i): {'start_date': "", "end_date": ""} for i in self.matches} for s in self.seasons} for l in
            self.leagues}

        failure = []

        for league in self.leagues:
            for season in self.seasons:
                season_start_date = None
                for match in self.matches:
                    try:

                        url = 'https://www.transfermarkt.com/jumplist/spieltag/wettbewerb/{}/saison_id/{}/spieltag/{}'.format(
                            league_to_code[league], season, match)

                        res = requests.get(url, headers={'User-Agent': 'Custom'})

                        soup = BeautifulSoup(res.content, 'html.parser')

                        season_matches = list(filter(lambda d: 'datum' in d.find('a')['href'],
                                                     soup.findAll('td', attrs={'class': 'zentriert no-border'})))
                        if match == 1:
                            date = season_matches[0].find('a')['href'].split("/")[-1]
                            season_start_date = date
                        date = season_matches[-1].find('a')['href'].split("/")[-1]
                        fetch_configs[league][season]["match_" + str(match)]['end_date'] = date
                        fetch_configs[league][season]["match_" + str(match)]['start_date'] = season_start_date
                    except:
                        failure.append([league, season, match])
        if save:
            json.dump(fetch_configs, open("football_dates_config.json", "w"))

        return fetch_configs


    def make_understat_request(self, league, season, start_date, end_date):
        headers = {
            'authority': 'understat.com',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'x-requested-with': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'sec-ch-ua-platform': '"macOS"',
            'origin': 'https://understat.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://understat.com/league/{}/{}'.format(league, season),
            'accept-language': 'en-US,en;q=0.9',
            'cookie': 'PHPSESSID=8f528cfaccd1a071fc39c3a64bcbe2f1; UID=a1e07340b4a6e612; _ga=GA1.2.225627759.1635602095; _gid=GA1.2.1356257761.1635602095; __gads=ID=fe68addb70b77992-22cd6b67a2cc00bf:T=1635602095:RT=1635602095:S=ALNI_MbxYzijkAyaCP6FMyEjGLQjGVCWOw; PROMOTIONS=eyI3Ijp7InZpZXdzIjo3LCJjbGlja3MiOjB9LCJkYXRldGltZSI6MTYzNTYwNjg3MDI0OH0=',
        }
        data = {
            'league': '{}'.format(league),
            'season': '{}'.format(season),
            'date_start': '{} 23:00:00'.format(start_date),
            'date_end': '{} 23:00:00'.format(end_date)
        }

        try:
            response = requests.post('https://understat.com/main/getPlayersStats/', headers=headers, data=data)
            return json.loads(response.content).get('response').get('players')
        except Exception as err:
            print(err)
            return None

    def scrape_players_data(self, save=False):
        flat_configs = [(l, s, self.dates_config[l][s]["match_" + str(m)]['start_date'],
                            self.dates_config[l][s]["match_" + str(m)]['end_date'], m) for l in self.leagues for s in self.seasons for
                           m in self.matches]
        frames = []

        for league, season, start, end, match in flat_configs:
            if isin_future(end):
                continue
            try:
                response = self.make_understat_request(league, season, start, end)
                if response:
                    df = pd.DataFrame(response)
                    df['League'] = league
                    df['Season'] = season
                    df['aggregated_to_match'] = match
                    frames.append(df)
            except:
                continue

            players_df = pd.concat(frames)
            if save:
                players_df.to_csv("../data/understat_players_df.csv")
            return players_df


    def get_players_data(self, load_exits=True, save=True):
        if load_exits:
            try:
                players_df = pd.read_csv("../data/understat_players_df.csv")
                return players_df
            except Exception as err:
                print("Failed to load data, Scraping instead..")
                return self.scrape_players_data(save)
        return self.scrape_players_data(save)


    def scrape_teams_data(self, save=True):
        frames = []
        base_url = 'https://understat.com/league'
        for league in self.leagues:
            for season in self.seasons:
                for match in self.matches:
                    url = base_url + '/' + league + '/' + season
                    try:
                        res = requests.get(url)
                    except Exception as err:
                        print(err)
                        print("Failed to fetch {} {} {}".format(league,season, match))
                        continue
                    soup = BeautifulSoup(res.content, "lxml")
                    scripts = soup.find_all('script')
                    string_with_json_obj = ''
                    for el in scripts:
                        if 'teamsData' in el.text:
                            string_with_json_obj = el.text.strip()

                    ind_start = string_with_json_obj.index("('") + 2
                    ind_end = string_with_json_obj.index("')")
                    json_data = string_with_json_obj[ind_start:ind_end]
                    json_data = json_data.encode('utf8').decode('unicode_escape')

                    data = json.loads(json_data)
                    teams_data = []
                    for team_id in data.keys():
                        teamName = data[team_id]['title']
                        team_data = data[team_id]['history'][:match]
                        df = pd.DataFrame(team_data)

                        df['ppda_coef'] = df['ppda'].apply(lambda d: d['att'] / d['def'] if d['def'] != 0 else 0)
                        df['oppda_coef'] = df['ppda_allowed'].apply(
                            lambda d: d['att'] / d['def'] if d['def'] != 0 else 0)

                        cols_to_sum = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored', 'missed', 'xpts',
                                       'wins', 'draws', 'loses', 'pts', 'npxGD']
                        cols_to_mean = ['ppda_coef', 'oppda_coef']

                        ## TODO Check what more columns exists

                        sum_data = pd.DataFrame(df[cols_to_sum].sum()).transpose()
                        mean_data = pd.DataFrame(df[cols_to_mean].mean()).transpose()
                        df_agg = pd.concat([sum_data, mean_data], axis=1)
                        df_agg['matches'] = len(df)
                        df_agg['team'] = teamName
                        teams_data.append(df_agg)

                    final_df = pd.concat(teams_data)
                    final_df['season'] = season
                    final_df['league'] = league
                    final_df['aggregated_to_match'] = match
                    frames.append(final_df)
        teams_dataframe = pd.concat(frames)
        if save:
            teams_dataframe.to_csv("../data/understat_teams_df.csv")

        return teams_dataframe



    def get_teams_data(self, load_exists=True, save=True):
        if load_exists:
            try:
                teams_df = pd.read_csv("../data/understat_teams_df.csv")
                return teams_df
            except Exception as err:
                print("Failed to load data, Scraping instead..")
                return self.scrape_teams_data(save)
        return self.scrape_teams_data(save)


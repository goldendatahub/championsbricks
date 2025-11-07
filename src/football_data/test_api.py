import requests
import json
import os

api_token = os.getenv("FOOTBALL_DATA_API_TOKEN")
uri = 'https://api.football-data.org/v4/matches'
headers = { 'X-Auth-Token': api_token }

response = requests.get(uri, headers=headers)
for match in response.json()['matches']:
  print(match)
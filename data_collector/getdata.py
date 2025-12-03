#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv

# Load api key from .env
load_dotenv()
api_key = os.getenv('API_KEY')


url = f"https://api.torn.com/torn/?key={api_key}&comment=tornticker&selections=items"

response = requests.get(url)

data = response.json()
items = data['items']

print(items['1'])

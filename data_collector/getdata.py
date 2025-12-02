#!/usr/bin/env python3

import requests

url = "https://api.torn.com/torn/?key=o9KYG6DYGCU1uLer&comment=tornticker&selections=items"

response = requests.get(url)

print(response.json())

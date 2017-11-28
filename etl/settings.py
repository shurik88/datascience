import json

def get():
    return json.load(open('settings.json'))
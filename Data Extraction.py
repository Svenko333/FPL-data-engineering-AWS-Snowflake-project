import os #paket koji se koristi za pozivanje Environment varijable
import requests, json
from datetime import datetime
import pandas as pd
import boto3 #paket pomoću kojeg premještamo fajle po folderima

def lambda_handler(event, context):
    # base url for all FPL API endpoints
    base_url = 'https://fantasy.premierleague.com/api/'
    
    varijabla_1 = False
    varijabla_2 = False
    current_date = datetime.now().date()
    
    # get data from bootstrap-static endpoint
    r = requests.get(base_url+'event-status/').json()
    for x in r["status"]:
        varijabla_1 = x["bonus_added"]
        last_date = x["date"]
    
    last_date = datetime.strptime(last_date, '%Y-%m-%d').date()
    if (current_date - last_date).days==1:
        varijabla_2=True
    

    if varijabla_1==True and varijabla_2==True:
        league_id = os.environ.get('league_id') #pozivanje Environment varijable
    
        data = requests.get(base_url+'leagues-classic/' + league_id + '/standings/').json()
        p = requests.get(base_url+'bootstrap-static/').json()
        is_finished = True
        i = 0
        while is_finished==True :
            if p["events"][i]["finished"] == False :
                is_finished = False
                gameweek = 'Gameweek ' + str(i)  
            i = i+1
        data["league"]["Gameweek"] = gameweek
        
        #print(data)
    
    
        #dobivenu fajlu treba prebaciti u odgovarajuci folder
        client = boto3.client('s3') #objekt u koji ćemo spustiti fajlu
        
        ime_bucketa = 'fpl-2024-2025'
        ime_foldera = 'raw_data/to_process/'
        ime_fajle = 'fpl_raw_data_' + str(datetime.now()) + '.json'
        fajla = json.dumps(data) #kako bi fajla bila u json obliku
        
        #naredba za spuštanje fajle u folder
        client.put_object(
            Bucket = ime_bucketa,
            Key = ime_foldera + ime_fajle,
            Body = fajla
            )
    else:
        print("Nema loada danas.")

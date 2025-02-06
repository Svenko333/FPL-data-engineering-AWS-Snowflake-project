import json
import boto3 #paket pomoću kojeg premještamo fajle po folderima
from datetime import datetime
import pandas as pd
from io import StringIO

#funkcija za transformaciju podataka iz fajle
def get_league_data(Data) : 
    league_data = []
    liga = Data["league"] 
    league_id = liga["id"]
    league_name = liga["name"]
    date_created = liga["created"]
    is_close_flag = liga["closed"]
    has_cup_flag = liga["has_cup"]
    gameweek = liga["Gameweek"]
    #insert date part
    now = datetime.now()
    date_int = int(now.strftime("%Y%m%d"))
    for natjecatelji in Data["standings"]["results"] :
        ime = natjecatelji["player_name"]
        natjecatelj_id = natjecatelji["entry"]
        ranking = natjecatelji["rank"]
        broj_bodova = natjecatelji["total"]
        ime_ekipe = natjecatelji["entry_name"]
        league_elements = {"league_id" : league_id, "league_name" : league_name, "date_created" : date_created, 
                           "gameweek" : gameweek,  "is_close_flag" : is_close_flag, "has_cup_flag" : has_cup_flag,
                           "manager_name" : ime, "manager_id" : natjecatelj_id, "ranking" : ranking, 
                           "total_points" : broj_bodova,  "team_name" : ime_ekipe, "insert_date_id" : date_int}
        league_data.append(league_elements)
    return league_data

def lambda_handler(event, context):
    s3 = boto3.client('s3') #objekt/folder iz kojeg cemo iscitati fajle
    ime_bucketa = "fpl-2024-2025"
    ime_foldera = "raw_data/to_process/"
    
    Data = []
    Keys = []
    
    #procitajmo sve fajle iz foldera
    for file in s3.list_objects(Bucket=ime_bucketa, Prefix = ime_foldera)['Contents'] :
        file_key = file["Key"] #ime fajle
        if file["Key"].split(".")[-1] == "json" : 
            json_fajla = s3.get_object(Bucket = ime_bucketa, Key = file_key)
            json_objekt = json_fajla["Body"] #da bi dobili tu json instancu koju tražimo
            sadrzaj = json.loads(json_objekt.read()) #citamo json fajlu i podatke koji se nalaze u njoj
            Data.append(sadrzaj)
            Keys.append(file_key)
            #print(sadrzaj)
    
    #transformirajmo podatke iz fajle i prebacimo ih u drugi folder        
    for data in Data : 
        league_data = get_league_data(data)
        league_df = pd.DataFrame.from_dict(league_data)
        league_df["date_created"] = pd.to_datetime(league_df["date_created"])
        
        league_buffer = StringIO() #naredba za pretvoriti dataframe u String obliku, cesto ce koristi kada
        league_df.to_csv(league_buffer, index=False) #iz string oblika u csv-icu
        league_content = league_buffer.getvalue() #naredba za izvuci sadrzaj iz String fajle
        
        ime_foldera_za_ligu = 'transformed_data/fpl_standings/'
        ime_fajle = 'league_data_transformed_' + str(datetime.now()) + '.csv'
        
        #prebacivanje fajle u drugi folder
        s3.put_object(
            Bucket = ime_bucketa,
            Key = ime_foldera_za_ligu + ime_fajle,
            Body = league_content
            )
    
    #sada jos moramo fajle "originalne" fajle koje smo transformirali, prebaciti ih u drugi folder kako ih ne bi opet obrađivali
    s3_resurs = boto3.resource('s3')
    
    for key in Keys:
        dictionary_sourcea_fajle = {
            "Bucket" : ime_bucketa,
            "Key" : key
        }
        #naredba za kopiranje fajle u novi folder("processed")
        s3_resurs.meta.client.copy(dictionary_sourcea_fajle, ime_bucketa, "raw_data/processed/" + key.split("/")[-1])
        s3_resurs.Object(ime_bucketa, key).delete() #brisanje fajle nakon sto je ona iskopirana u drugi fo
    
    

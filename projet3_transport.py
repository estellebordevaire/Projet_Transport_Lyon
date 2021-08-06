import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime, timedelta
import schedule

# Initialisation des DataFrames
df = pd.DataFrame(columns = ["idtarretdestination", "coursetheorique", "direction", "ligne", "delaipassage",
                             "heurepassage", "gid", "last_update_fme", "type", "id", "time_download_DDHHMMSS"])

df1 = pd.DataFrame(columns = ['type', 'debut', 'ligne_com', 'mode', 'titre', 'ligne_cli', 'message',
                              'cause', 'last_update_fme', 'fin', 'time_download_DDHHMMSS'])

# Planification du schedule
start = datetime(2021,8,6,19,57)
end = start + timedelta(hours=7)


def current_time():
    now = datetime.now()
    return now.strftime("%d_%H_%M_%S")


# Fonction qui récupère les données Prochains Passages
def retrieve_data():
    global df
    username = 'estellebordevaire@yahoo.fr'
    password = 'projet3WCS!'
    url = 'https://download.data.grandlyon.com/ws/rdata/tcl_sytral.tclpassagearret/all.json?maxfeatures=-1&start=1'

    r=requests.get(url, auth=HTTPBasicAuth(username, password))
    data=r.json()
    
    passage=pd.json_normalize(data["values"])
    passage["Time_downLoad_DDHHMMSS"] = current_time()

    df = pd.concat([df, passage])

    df.to_csv("Lyon_passagearret_EB1.csv", index = False)

    return df


# Fonction qui récupère les données Alertes Traffic
def retrieve_alerte():
    global df1
    username = 'estellebordevaire@yahoo.fr'
    password = 'projet3WCS!'
    url1 = 'https://download.data.grandlyon.com/ws/rdata/tcl_sytral.tclalertetrafic_2/all.json?maxfeatures=-1&start=1'

    r1=requests.get(url1, auth=HTTPBasicAuth(username, password))
    data1=r1.json()
    
    alerte=pd.json_normalize(data1["values"])
    alerte["Time_downLoad_DDHHMMSS"] = current_time()

    df1 = pd.concat([df1, alerte])
    
    df1.to_csv("Lyon_alertetrafic_EB1.csv", index = False)

    return df1


#schedule.every(1).minutes.do(retrieve_data)

# Traitement de la fin du schedule :
#while datetime.now() < end:
#    if datetime.now() > start:
#        schedule.run_pending()
        
while datetime.now() < start:
#    test="oups"
    time.sleep(59)
else:
    schedule.every(1).minutes.do(retrieve_data)
    schedule.every(5).minutes.do(retrieve_alerte)
    while datetime.now() < end :
        schedule.run_all()
      
                    
# Sauvegarde du dataframe complet
#df.to_csv("Lyon_passagearret_EBTot.csv", index = False)

#Lancer le script Python depuis le Terminal Anaconda Prompt : 
# c:\Users\cyril\Documents\Anaconda\python.exe c:\Users\cyril\Documents\Python_Scripts\projet3_transport.py
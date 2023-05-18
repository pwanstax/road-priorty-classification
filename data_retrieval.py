import requests
import pandas as pd

def get_traffy_road():

    url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?problem_type_abdul=%E0%B8%96%E0%B8%99%E0%B8%99&state_type=start'

    response = requests.get(url)

    if response.status_code == 201:
        data = response.json()
        results = data['results']
        traffy_road_df = pd.DataFrame(results)

        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.max_colwidth', None)
        traffy_road_df.to_csv('road.csv', index=False)
        print(traffy_road_df.head(5))
    else:
        print("Error occurred while retrieving data.")
    
    return traffy_road_df

def get_traffic():
    url = 'https://data.bangkok.go.th/api/3/action/datastore_search?limit=265&resource_id=b5c597aa-f36c-4cbd-8cde-c208e3b2236c' # ข้อมูลจุดฝืดด้านการจราจรบนถนน 3 ปีย้อนหลัง

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        records = data['result']['records']
        traffic_df = pd.DataFrame(records)
        traffic_df = traffic_df.drop(columns=["_id", "No"])

        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.max_colwidth', None)
        traffic_df.to_csv('traffic.csv', index=False)
        print(traffic_df.shape[0])
        print(traffic_df.head(10))
    else:
        print("Error occurred while retrieving data.")

    return traffic_df

def get_accident():
    url = 'https://data.bangkok.go.th/api/3/action/datastore_search?resource_id=6cc7a43f-52b3-4381-9a8f-2b8a35c3174a' # ข้อมูลจุดเสี่ยงอุบัติเหตุสูงสุด 3 ปีย้อนหลัง

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        records = data['result']['records']
        accident_df = pd.DataFrame(records)
        accident_df = accident_df.drop(columns=["_id", "ลำดับ"])

        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('display.max_colwidth', None)
        accident_df.to_csv('accident_data.csv', index=False)
        print(accident_df.head(10))
    else:
        print("Error occurred while retrieving data.")
    
    return accident_df

def get_data():

    traffy_road_df = get_traffy_road()
    traffy_traffic_df= get_traffic()
    accident_df = get_accident()

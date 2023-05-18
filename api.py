from fastapi import FastAPI, Response
from pydantic import BaseModel
import json
from typing import List
from data_retrieval import get_data
from preprocessing import preprocess_data
from clustering import perform_clustering


app = FastAPI()

class Item(BaseModel):
    ticket_id: str
    address: str
    Latitude: str
    Longitude: str
    hours_ago: int
    ID: int
    trafficCount: int
    accidentCount: int


@app.get('/labels')
async def labels():
    get_data()
    traffy_road_prep = preprocess_data()
    kmeans = perform_clustering(traffy_road_prep)

    return {'labels': kmeans.labels_.tolist()}

@app.get("/data")
async def data():
    get_data()
    traffy_road_prep = preprocess_data()
    
    data_list = [row.asDict() for row in traffy_road_prep.collect()]

    headers = {
        "content-type": "application/json; charset=utf-8",
    }

    return Response(content=json.dumps(data_list, ensure_ascii=False), headers=headers)

@app.get('/')
async def test():
    return "Connected"
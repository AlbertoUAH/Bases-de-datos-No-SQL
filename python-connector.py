import pymongo
import os 
import folium
from folium import plugins
from bson.son import SON
MAX_DISTANCE = 400000

connection_string = 'mongodb://localhost'
connection = pymongo.MongoClient(connection_string)
database = connection.practica_final

def prueba1():
    coordinates =  [-73.99279, 40.719296]
    pipe = [{
            "$geoNear": {
                "near": { "type": "Point", "coordinates": coordinates},
                "distanceField": "dist.calculated",
                "maxDistance": MAX_DISTANCE,
                "includeLocs": "dist.location",
                "spherical": "true"
            }
        }, 
        {
            "$group": {"_id": {"Location": "$dist.location", "Airport": "$Airport.Name"}}
        }]

    result = database.prueba.aggregate(pipe)
    list_coordinates = [[item['_id']['Location']['coordinates'], item['_id']['Airport']] for item in result]

    m = folium.Map()
    folium.Marker(
            location=coordinates[::-1], # coordinates for the marker (Earth Lab at CU Boulder)
            popup='You\'re right here', # pop-up label for the marker
            icon=folium.Icon(color='red'),
        ).add_to(m)

    for coordinate in list_coordinates:
        folium.Marker(
            location=coordinate[0][::-1], # coordinates for the marker (Earth Lab at CU Boulder)
            popup=coordinate[1], # pop-up label for the marker
            icon=folium.Icon()
        ).add_to(m)

    folium.Circle(
        location=coordinates[::-1],
        radius=MAX_DISTANCE,
        color='#3186cc',
        fill=True,
        fill_color='#3186cc'
    ).add_to(m)
    m.save('plot_data.html')

def prueba2():
    coordinates = [ -120.426935, 34.939985 ]
    pipe = [
    {
        "$geoNear": {
            "near": { "type": "Point", "coordinates": coordinates },
            "distanceField": "dist.calculated",
            "key": "Position",
            "includeLocs": "dist.location",
            "spherical": "true"
        }
    },
    {
        "$group": {
            "_id": {"Location": "$dist.location", "Airport": "$Airport.Name"}, "minutos": {"$sum": "$Statistics.Minutes Delayed.Total"}, "vuelos": {"$sum": "$Statistics.Flights.Total"}
        }
    },
    {
        "$project": {"_id": 1, "proporcion_minutos_vuelos": {"$divide": ["$minutos", "$vuelos"]}}
    },
    {
        "$match": {"$expr": {"$lte": ["$proporcion_minutos_vuelos", 10]}}
    }
    ]
    result = database.prueba.aggregate(pipe)
    list_coordinates = [[item['_id']['Location']['coordinates'], item['_id']['Airport'], item['proporcion_minutos_vuelos']] for item in result]

    m = folium.Map()
    folium.Marker(
            location=coordinates[::-1], # coordinates for the marker (Earth Lab at CU Boulder)
            popup='You\'re right here', # pop-up label for the marker
            icon=folium.Icon(color='red'),
        ).add_to(m)

    for coordinate in list_coordinates:
        folium.Marker(
            location=coordinate[0][::-1], # coordinates for the marker (Earth Lab at CU Boulder)
            popup=str(coordinate[1] + " - " + str(round(coordinate[2], 2))), # pop-up label for the marker
            icon=folium.Icon()
        ).add_to(m)
    m.save('plot_data.html')

prueba2()
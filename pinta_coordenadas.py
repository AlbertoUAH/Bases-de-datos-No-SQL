import pymongo
import os 
import folium
from folium import plugins
from bson.son import SON

# Inicialmente nos conectamos al servidor local
connection_string = 'mongodb://localhost'
connection = pymongo.MongoClient(connection_string)
# A continuacion, nos conectamos a la base de datos practica_final
database = connection.practica_final

def calcular_distancia(campo):
    """
    Funcion que calcular la distancia de los tres aeropuertos con menor
    media de minutos de demora en funcion del campo elegido 
    (CONSULTA 9 del script de MongoDB)
    Campo puede ser:
        "$Statistics.MinutesDelayed.Carrier"
        "$Statistics.MinutesDelayed.LateAircraft"
        "$Statistics.MinutesDelayed.NationalAviationSystem"
        "$Statistics.MinutesDelayed.Security"
        "$Statistics.MinutesDelayed.Weather"
    Coordenada utilizada: Wichita (Kansas)

    IMPORTANTE: las coordenadas estan en el orden longitud, latitud; por lo que
    antes de mostrar las coordenadas en el mapa debemos invertir el orden (latitud, longitud) [::-1]
    """
    coordinates =  [-97.347059 , 37.631635]
    pipe = [{
            "$geoNear": {
                "near": { "type": "Point", "coordinates": coordinates},
                "distanceField": "dist.calculated",
                "includeLocs": "dist.location",
                "distanceMultiplier": 0.001,
                "spherical": "true"
            }
        }, 
        {
            "$group": {"_id": {"Location": "$dist.location", "Airport": "$Airport.Name", "Distance": "$dist.calculated"}, "Minutes_Delayed": {"$avg": campo}}
        },
        {
            "$sort": {"Minutes_Delayed": 1}
        },
        {
            "$limit": 3
        }]

    result = database.airports.aggregate(pipe)
    list_coordinates = [[item['_id']['Location']['coordinates'], item['_id']['Airport'], item['Minutes_Delayed'], item['_id']['Distance']] for item in result]

    m = folium.Map()
    # Con Marker creamos un marcador (punto) en el mapa
    # Este primer marcador senala la posicion de referencia
    folium.Marker(
            location=coordinates[::-1],
            popup='You\'re right here',
            icon=folium.Icon(color='red'),
        ).add_to(m)

    # Mediante un bucle, creamos un marcador por cada aeropuerto
    # incluyendo en un pop-up tanto el nombre del aeropuerto (coordinate[1])
    # como la media de minutos de demora (coordinate[2])
    for coordinate in list_coordinates:
        folium.Marker(
            location=coordinate[0][::-1],
            popup=coordinate[1] + "\n" + str(coordinate[2]),
            icon=folium.Icon()
        ).add_to(m)
        aux_list = [coordinates[::-1], coordinate[0][::-1]]

        # Con PolyLine creamos una linea entre dos coordenadas
        # incluyendo en un pop-up la distancia (en kilometros)
        # redondeada a dos cifras decimales (coordinate[3])
        folium.PolyLine(
            aux_list,
            color = "red",
            popup=str(round(coordinate[3], 2)) + " km."
        ).add_to(m)
        aux_list = []

    # Finalmente, guardamos el mapa en un fichero HTML
    m.save('plot_data.html')

# Pruebas - calcular_distancia
# calcular_distancia("$Statistics.MinutesDelayed.Carrier")
# calcular_distancia("$Statistics.MinutesDelayed.LateAircraft")
# calcular_distancia("$Statistics.MinutesDelayed.NationalAviationSystem")
# calcular_distancia("$Statistics.MinutesDelayed.Security")
# calcular_distancia("$Statistics.MinutesDelayed.Weather")

def aeropuerto_mas_cercano():
    """
    Funcion que calcula el aeropuerto mas cercano (a 500 km o menos de distancia)
    cuya proporcion minutos_demora / vuelos_demorados sea igual o inferior a 50
    (CONSULTA 10 del script de MongoDB)
    Coordenada utilizada: Santa Maria (California)

    IMPORTANTE: las coordenadas estan en el orden longitud, latitud; por lo que
    antes de mostrar las coordenadas en el mapa debemos invertir el orden (latitud, longitud) [::-1]
    """
    coordinates = [ -120.426935, 34.939985 ]
    pipe = [
    {
        "$geoNear": {
            "near": { "type": "Point", "coordinates": coordinates },
            "distanceField": "dist.calculated",
            "key": "Position",
            "maxDistance": 500000,
            "distanceMultiplier": 0.001,
            "includeLocs": "dist.location",
            "spherical": "true"
        }
    },
    {
        "$group": {
            "_id": {"Location": "$dist.location", "Airport": "$Airport.Name", "Distance": "$dist.calculated"}, 
            "minutos": {"$sum": "$Statistics.MinutesDelayed.Total"}, 
            "vuelos": {"$sum": "$Statistics.Flights.Delayed"}
        }
    },
    {
        "$project": {"_id": 1, "proporcion_minutos_vuelos": {"$divide": ["$minutos", "$vuelos"]}}
    },
    {
        "$match": {"$expr": {"$lte": ["$proporcion_minutos_vuelos", 50]}}
    }
    ]
    result = database.airports.aggregate(pipe)
    list_coordinates = [[item['_id']['Location']['coordinates'], item['_id']['Airport'], item['proporcion_minutos_vuelos'], item['_id']['Distance']] for item in result]

    m = folium.Map()
    folium.Marker(
            location=coordinates[::-1], # coordinates for the marker (Earth Lab at CU Boulder)
            popup='You\'re right here', # pop-up label for the marker
            icon=folium.Icon(color='red'),
        ).add_to(m)

    for coordinate in list_coordinates:
        folium.Marker(
            location=coordinate[0][::-1], 
            popup=str(coordinate[1] + " - " + str(round(coordinate[2], 2))),
            icon=folium.Icon()
        ).add_to(m)
        aux_list = [coordinates[::-1], coordinate[0][::-1]]
        folium.PolyLine(
            aux_list,
            color = "red",
            popup=str(round(coordinate[3], 2)) + " km."
        ).add_to(m)
        aux_list = []
    m.save('plot_data.html')

# Prueba aeropuerto_mas_cercano
# aeropuerto_mas_cercano()
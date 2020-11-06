use practica_final

// Empezamos por unas consultas mas sencillas
// Contar el numero de documentos
db.airports.count()

// ------------------------------------ PREPROCESAMIENTO ------------------------------------
db.airports.updateMany({},{$unset: {"Time.Label": ""}})
db.airports.updateMany({},{$unset: {"Time.Month Name": ""}})

// Probamos a insertar un dato
var aeropuerto = { "Code" : "SLC", "Name" : "Salt Lake City, UT: Salt Lake City International" }
var tiempo = { "Month" : 1, "Year" : 2016 }
var demoras = { "Carrier" : 368, "Late Aircraft" : 549, "National Aviation System" : 253, "Security" : 9, "Weather" : 37 }
var companias = {
	"Names" : "American Airlines Inc.,Alaska Airlines Inc.,JetBlue Airways,Delta Air Lines Inc.,Frontier Airlines Inc.,SkyWest Airlines Inc.,United Air Lines Inc.,Southwest Airlines Co.",
	"Total" : 8
}
var vuelos = { "Cancelled" : 81, "Delayed" : 1170, "Diverted" : 12, "On Time" : 7424, "Total" : 8690 }
var minutos_demora = { "Carrier" : 32066, "Late Aircraft" : 33682, "National Aviation System" : 8057, "Security" : 57, "Total" : 76978, "Weather" : 3116 }
var estadisticas = { "# of Delays": demoras, "Carriers": companias, "Flights": vuelos, "Minutes Delayed" : minutos_demora}
db.airports.insertOne({"Airport": aeropuerto, "Time": tiempo, "Statistics": estadisticas})

// Para no afectar a las estadisticas y las consultas, lo volvemos a eliminar
db.airports.remove({"_id": {$eq: ObjectId("5fa2d4a72d8abb02bb66793e")}})

// Renombramos el campo # of Delays a Delays
db.airports.updateMany({}, {$rename: {"Statistics.# of Delays": "Statistics.Delays"}})

// Sin embargo, tenemos un problema. El campo con las companias de vuelo estan en String, y nos gustaria modificarlo
// Actualizar el campo Carriers.Names, cambiando el string a formato array
var companias_aereas = {$split: ["$Statistics.Carriers.Names", ","]}
var project = {"Statistics.Carriers.Names": companias_aereas}
var fase1 = {$addFields: project}
var fase2 = {$out: "airports"}
db.airports.aggregate(fase1, fase2)

// Por otro lado, vemos que tanto los campos "Carriers", "Flights" como "Minutes Delayed"
// presentan un campo denominado "Total". Antes de trabajar con dicho campo, vamos a comprobar
// que se trata del total de companias, vuelos y minutos de demora, respectivamente

// Carriers
// Hay 4408 campos "Total"
var condicion = [{$eq: [ {$size: "$Statistics.Carriers.Names"}, "$Statistics.Carriers.Total"]}, 1, 0]
var coincidentes =  {"Coincidentes": {$cond: condicion}}
var fase1 = {$project: coincidentes}
var group = {_id: null, SumCoincidentes: {$sum: "$Coincidentes"}}
var fase2 = {$group: group}
var fase3 = {$project: {_id: 0}}
db.airports.aggregate([fase1, fase2, fase3])

// Flights y Minutes Delayed
function comprobar_total(raiz, claves, campo_total) {
    var array = [];
    claves.forEach(clave => array.push(raiz.concat(clave)));
    var suma = {$sum: array};
    var condicion = [{$eq: [suma , campo_total]}, 1, 0];
    var coincidentes =  {"Coincidentes": {$cond: condicion}};
    var fase1 = {$project: coincidentes};
    var group = {_id: null, SumCoincidentes: {$sum: "$Coincidentes"}};
    var fase2 = {$group: group};
    var fase3 = {$project: {_id: 0}};
    return db.airports.aggregate([fase1, fase2, fase3]);
}
comprobar_total("$Statistics.Flights.", ["Cancelled", "Delayed", "Diverted", "On Time"], "$Statistics.Flights.Total");
comprobar_total("$Statistics.Minutes Delayed.", ["Carrier", "Late Aircraft", "National Aviation System", "Security", "Weather"], "$Statistics.Minutes Delayed.Total");
comprobar_total("$Statistics.Delays.", ["Carrier", "Late Aircraft", "National Aviation System", "Security", "Weather"], "$Statistics.Flights.Delayed");

// Minutes Delayed y Flights.Delayed (no todos los valores son correctos), hay que corregirlos
var raiz = "$Statistics.Minutes Delayed."
var array_minutos = [raiz.concat("Carrier"), raiz.concat("Late Aircraft"), raiz.concat("National Aviation System"), raiz.concat("Security"), raiz.concat("Weather")]
var suma = {$sum: array_minutos}
var raiz = "$Statistics.Delays."
var array_delays = [raiz.concat("Carrier"), raiz.concat("Late Aircraft"), raiz.concat("National Aviation System"), raiz.concat("Security"), raiz.concat("Weather")]
var suma_delays = {$sum: array_delays}

var condicion = {$or: {$ne: [suma, "$Statistics.Minutes Delayed.Total"], $ne: [suma_delays, "$Statistics.Flights.Delayed"]}}
var fase1 = {$match: {$expr: condicion}}
var total = {"Statistics.Minutes Delayed.Total": suma, "Statistics.Flights.Delayed": suma_delays}
var fase2 = {$addFields: total}
var fase3 = {$project: {_id:1, total_minutes: "$Statistics.Minutes Delayed.Total", total_delays: "$Statistics.Flights.Delayed"}}

// Para corregirlos, mediante un updateMany actualizamos aquellas filas cuyo campo "total" no sea valido
db.airports.aggregate([fase1, fase2, fase3]).forEach(function(id){
    db.airports.updateMany({"_id": id._id}, {$set: {"Statistics.Minutes Delayed.Total": id.total_minutes, "Statistics.Flights.Delayed": id.total_delays})
})

// --------------------------------- OPERACIONES CRUD + AGGREGATE -------------------------------------------------
// -----------------------------------------------find()-----------------------------------------------------------
// CONSULTA 1. Consultar el codigo y nombre del aeropuerto con el mayor valor en una categoria, en un mes y anno en particular
function mayor_valor_categoria(mes, anno, campo) {
    var valor = "$".concat(campo.toString())
    var query = {"Time.Year": anno, "Time.Month": { $eq: mes}}
    var select = {_id: 0, "Code": 1, "Airport.Code": 1, "Airport.Name": 1, "Total" : valor}
    return db.airports.find(query, select).sort({[campo] : -1]}).limit(5)
}
// Pruebas
mayor_valor_categoria(1, 2010, "Statistics.Flights.Total")
// Coincidente con el tiroteo producido en el aeropuerto internacional de Los Angeles en noviembre del anno 2013
// URL: https://en.wikipedia.org/wiki/2013_Los_Angeles_International_Airport_shooting
mayor_valor_categoria(11, 2013, "Statistics.Delays.Security")
mayor_valor_categoria(11, 2013, "Statistics.Flights.Diverted")

// aggregate()
// CONSULTA 2. Analizar el numero total de vuelos realizados en cada anno
var subquery = {$sum: "$Statistics.Flights.Total"}
var query = {_id: "$Time.Year", vuelos: subquery}
var fase1 = {$group: query}
var fase2 = {$sort: {"_id": 1}}
db.airports.aggregate([fase1, fase2])

// Para calcular la diferencia de vuelos entre dos años
db.airports.aggregate([fase1, fase2]).forEach(function(doc) {
    var query_aux = {"$gt": doc._id}
    var subfase = {$match: {"Time.Year": query_aux}}
    db.airports.aggregate([subfase, fase1, fase2]).limit(1).forEach(function(doc_aux) {
        var diferencia = doc_aux.vuelos - doc.vuelos
        var texto = ("Diferencia entre " + doc._id + " y " + doc_aux._id).toString()
        print(texto + ": " + diferencia)
    })
})

// CONSULTA 3. Consultar el top 10 aeropuertos con el mayor valor en un campo, junto con su porcentaje
function aeropuerto_mes_anno(campo) {
    var fase1 = {$group: {_id: null, total: {$sum: campo}, aeropuerto: { $push: "$$ROOT" }}}
    var fase2 = {$unwind: "$aeropuerto"}
    var fase3 = {$project: {_id: "$aeropuerto._id", "Airport": "$aeropuerto.Airport", "Statistics": "$aeropuerto.Statistics", "total": "$total"}}
    var group = {_id: "$Airport.Name", vuelos: {$sum: campo}, total: {$first: "$total"}}
    var fase4 = {$group: group}
    var fase5 = {$sort: {"vuelos": -1}}
    var porcentaje = {"$multiply": [{"$divide": ["$vuelos", "$total"]}, 100]}
    var field = {"porcentaje (%)": porcentaje}
    var fase6 = {$addFields: field}
    var fase7 = {$project: {"total": 0}}
    return db.airports.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7]).limit(10)
}
aeropuerto_mes_anno("$Statistics.Flights.Cancelled")
aeropuerto_mes_anno("$Statistics.Flights.On Time")
aeropuerto_mes_anno("$Statistics.Flights.Delayed")
aeropuerto_mes_anno("$Statistics.Delays.Weather")
aeropuerto_mes_anno("$Statistics.Minutes Delayed.Weather")

// CONSULTA 4. Analizando el resultado anterior, quisieramos obtener el mes que mas cancelaciones se realizan de media por aeropuerto
var ids = {id_aeropuerto: "$Airport.Name", id_mes: "$Time.Month"}
var media = {$avg: "$Statistics.Flights.Cancelled"}
var query1 = {_id: ids, media_cancelaciones: media}
var fase1 = {$group: query1}
var redondeo = {$round: ["$media_cancelaciones", 2]}
var push = {$push: {mes: "$_id.id_mes", media_cancelaciones: redondeo}}
var group = {_id: "$_id.id_aeropuerto", parejas: push}
var fase2 = {$group: group}
var fase3 = {$unwind: "$parejas"}
var fase4 = {$sort: {"parejas.media_cancelaciones": -1}}
var nuevo_group = {_id: "$_id", parejas: {$first: "$parejas"}}
var fase5 = {$group: nuevo_group}
var merge = [ { aeropuerto: "$_id"}, "$parejas" ]
var fase6 = { $replaceWith: { $mergeObjects: merge } }
var fase7 = {$sort: {"media_cancelaciones": -1}}
db.airports.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])

// Si lo agrupamos por meses
var fase1 = {$group: {_id: "$Time.Month", media_cancelaciones: {$avg: "$Statistics.Flights.Cancelled"}}}
var fase2 = {$sort: {"media_cancelaciones": -1}}
db.airports.aggregate([fase1, fase2])

// CONSULTA 5. Consultar el aeropuerto con mayor y menor proporcion minutos_demora / total_demoras
var suma_delays = {$sum: "$Statistics.Flights.Delayed"}
var suma_minutes_delayed = {$sum: "$Statistics.Minutes Delayed.Total"}
var group = {_id: "$Airport.Name", minutes_delayed: suma_minutes_delayed, delays: suma_delays }
var fase1 = {$group: group}
var division = {$divide: ["$minutes_delayed", "$delays"]}
var fase2 = {$project: {_id: 1, "Proportion": division}}
var fase3 = {$sort: {"Proportion": -1}}
var nuevo_group = {_id: null, "Most_Proportion": {$first: "$$ROOT"}, "Less_Proportion": {$last: "$$ROOT"}}
var fase4 = {$group: nuevo_group}
var fase5 = {$project: {_id: 0}}
db.airports.aggregate([fase1, fase2, fase3, fase4, fase5])

// CONSULTA 6. Analizar el numero medio de companias aereas que ha habido a lo largo de los annos
var fase1 = {$group: {_id: "$Time.Year", companias: {$avg: "$Statistics.Carriers.Total"}}}
var fase2 = {$sort: {"_id": 1}}
var fase3 = {$project: {_id: "$_id", companias: {$round: ["$companias", 0]}}}
db.airports.aggregate([fase1, fase2, fase3])

// CONSULTA 7. Consultar companias que desaparecieron entre dos annos consecutivos
var fase1 = {$project: {anno: "$Time.Year", companias: "$Statistics.Carriers.Names"}}
var fase2 = {$unwind: "$companias"}
var fase3 = {$group: {_id: "$anno", companias: {$addToSet: "$companias"}}}
var fase4 = {$project: {_id: 0, "companias_anno": {"anno": "$_id", "companias": "$companias"}}}
var fase5 = {$sort: {"companias_anno.anno": 1}}
var fase6 = {$group: {_id: null, companias_anno: {$push: "$companias_anno"}}}
var longitud = {$size: "$companias_anno"}
var fase7 = {$project: {_id: 0 , "companias_anno": 1, "companias_anno_menos_1": {$slice: ["$companias_anno", 1, longitud]}}}
var fase8 = {$project: {"parejas": {$zip: { "inputs": [ "$companias_anno", "$companias_anno_menos_1"] }}}}
var fase9 = {$unwind: "$parejas"}
var elem0 = {anno: {$arrayElemAt: ["$parejas.anno", 0]}, diferencia: {$arrayElemAt: ["$parejas.companias", 0]}}
var elem1 = {anno: {$arrayElemAt: ["$parejas.anno", 1]}, diferencia: {$arrayElemAt: ["$parejas.companias", 1]}}
var fase10 = {$project: {elem0, elem1}}
var diferencia = {$setDifference: ["$elem0.diferencia", "$elem1.diferencia"]}
var fase11 = {$project: {"annos": ["$elem0.anno", "$elem1.anno"] , "diferencia": diferencia}}
db.airports.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8, fase9, fase10, fase11])

// CONSULTA 8. Consultar que compañias que se han ido manteniendo con el transcurso de los annos
var fase1 = {$project: {anno: "$Time.Year" , companias: "$Statistics.Carriers.Names"}}
var fase2 = {$unwind: "$companias"}
var fase3 = {$group: {_id: "$anno", companias: {$addToSet: "$companias"}}}
var fase4 = {$project: {_id: 0, "companias_anno": {"anno": "$_id", "companias": "$companias"}}}
var fase5 = {$sort: {"companias_anno.anno": 1}}
var group = {_id: 0, "companias": {"$push": "$companias_anno.companias"}, "primerasCompanias": {"$first": "$companias_anno.companias"}}
var fase6 = {$group: group}
var interseccion = {$setIntersection: ["$$value", "$$this"]}
var reduce = {$reduce: {input: "$companias", initialValue: "$primerasCompanias", in: interseccion}}
var fase7 = {$project: {_id: 0, "companias": reduce}}
db.airports.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])

// --------------------------------------CODIGOS IATA--------------------------------------------------
// Consultamos el primer documento para ver cada uno de los campos
db.codigos_iata.find().limit(1)

// Aeropuerto situado a menos de 10 metros de altura
var sort = {"elevation_meters": -1}
var condicion = {"elevation_ft": {$gt: 3280.80}}
var proyeccion = {_id: 0, "name": 1, "elevation_ft": 1}
db.codigos_iata.find(condicion, proyeccion).sort(sort)

// A continuacion, creamos el campo "position", con el objetivo de crear un campo con las coordenadas
var coordenadas = {$split: ["$coordinates", ","]}
var coord0 = { $arrayElemAt: [ coordenadas, 0 ] }
var coord1 = { $arrayElemAt: [ coordenadas, 1 ] }
var elem0 = {$toDouble: {$trim: {input: coord0}}}
var elem1 = {$toDouble: {$trim: {input: coord1}}}
var posicion = {"Position": {"type": "Point", "coordinates": [elem0, elem1]}}
var fase1 = {$addFields: posicion}
var fase2 = {$out: "codigos_iata"}
db.codigos_iata.aggregate([fase1, fase2])

// A continuacion, ligamos ambas colecciones, mediante la funcion $lookup
var parametros = {from: "codigos_iata", localField: "Airport.Code", foreignField: "iata_code", as: "location"}
var fase1 = {$lookup: parametros}
var fase2 = {$addFields: {"Position": "$location.Position"}}
var posicion = {$arrayElemAt: ["$Position", 0]}
var fase3 = {$set: {"Position": posicion}}
var fase4 = { $project: { location: 0} }
var fase5 = {$out: "airports"}
db.airports.aggregate([fase1, fase2, fase3, fase4, fase5])

// Comprobacion
// Si no existen ...
db.airports.find({"Position":{ $exists: false}}, {"Position": 1}).count()
// Si estan a null ...
db.airports.find({"Position":{ $eq: null}}, {"Position": 1}).count()

// Creamos un indice con el que consultar las coordenadas. Debe estar en formato 2dsphere
db.airports.getIndexes()
db.airports.createIndex( { Position : "2dsphere" } )

function calcular_distancia(campo) {
    var parametros = {
            near: { type: "Point", coordinates: [ -73.99279 , 40.719296 ] },
            distanceField: "dist.calculated",
            includeLocs: "dist.location",
            spherical: true
         }
    var fase1 = {$geoNear: parametros}
    var media_minutes_delayed = {$avg: campo}
    var group = {_id: {"Location": "$dist.location", "Airport": "$Airport.Name", "Distance": "$dist.calculated"}, "Minutes_Delayed": media_minutes_delayed}
    var fase2 = {$group: group}
    var fase3 = {$sort: {"Minutes_Delayed": -1}}
    return db.airports.aggregate([fase1, fase2, fase3]).limit(3)
}
calcular_distancia("$Statistics.Minutes Delayed.Carrier")

var parametros = {
        near: { type: "Point", coordinates: [ -120.426935, 34.939985 ] },
        distanceField: "dist.calculated",
        key: "Position",
        maxDistance: 1000000,
        includeLocs: "dist.location",
        spherical: true
     }
var fase1 = {$geoNear: parametros}
var group = {_id: {"Location": "$dist.location", "Airport": "$Airport.Name", "Distance": "$dist.calculated"}, minutos: {$sum: "$Statistics.Minutes Delayed.Total"}, vuelos: {$sum: "$Statistics.Flights.Delayed"}}
var fase2 = {$group: group}
var project = {"_id": 1, "proporcion_minutos_vuelos": {$divide: ["$minutos", "$vuelos"]}}
var fase3 = {$project: project}
var cond = {$expr: {$lte: ["$proporcion_minutos_vuelos", 50]}}
var fase4 = {$match: cond}
db.airports.aggregate([fase1, fase2, fase3, fase4])
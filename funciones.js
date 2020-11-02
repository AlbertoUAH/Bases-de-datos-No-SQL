use practica_final

// Una vez descargado e importado el json, echamos un primer vistazo a los datos
db.pruebas.find()

// Empezamos por unas consultas mas sencillas
// Contar el numero de documentos
db.pruebas.count()

// Funcion que recupera las estadisticas de vuelos en un anno, mes y codigo de aeropuerto dados
// Se puede filtrar incluso por columnas
function obtener_vuelos_mes_anno(mes, anno, codigo, columnas = {}) {
    if (columnas == {})
        return db.pruebas.find({"Time.Month": mes, "Time.Year": anno, "Airport.Code": codigo});
    else
        return db.pruebas.find({"Time.Month": mes, "Time.Year": anno, "Airport.Code": codigo}, columnas)
}
// Prueba 1
obtener_vuelos_mes_anno(6, 2003, "CLT")

// Prueba 2
obtener_vuelos_mes_anno(6, 2003, "CLT", {"Statistics.Flights": 1})

// ------------------------------------ OPERACIONES CRUD ------------------------------------
// CONSULTA 1. Obtener los 3 primeros aeropuertos con menos minutos de demora en un anno determinado
function obtener_aeropuertos_menos_demora(anno, top) {
    var opcion = {"Statistics.Minutes Delayed.Total": -1}
    return db.pruebas.find({"Time.Year": anno}, {_id: 0 , "Airport.Name": 1, "Statistics.Minutes Delayed.Total": 1}).sort(opcion).limit(top).map( documento => 
      ({ aeropuerto: documento['Airport']['Name'],
         minutos: documento['Statistics']['Minutes Delayed']['Total']
          
      })
    )
}
// Prueba
obtener_aeropuertos_menos_demora(2016, 3)

// Sin embargo, tenemos un problema. El campo con las companias de vuelo estan en String, y nos gustaria modificarlo
// Actualizar el campo Carriers.Names, cambiando el string a formato array
var companias_aereas = {$split: ["$Statistics.Carriers.Names", ","]}
var project = {"Statistics.Carriers.Names": companias_aereas}
var fase1 = {$addFields: project}
var fase2 = {$out: "pruebas_modificado"}
db.pruebas.aggregate(fase1, fase2)

// En pruebas_modificado se encuentra el JSON con los nombres de las companias en formato array
db.pruebas_modificado.find()

// Por otro lado, vemos que tanto los campos "Carriers", "Flights" como "Minutes Delayed"
// presentan un campo denominado "Total". Antes de trabajar con dicho campo, vamos a comprobar
// que se trata del total de companias, vuelos y minutos de demora, respectivamente

// Carriers
db.pruebas_modificado.find().size()
// Hay 4408 campos "Total"
var condicion = [{$eq: [ {$size: "$Statistics.Carriers.Names"}, "$Statistics.Carriers.Total"]}, 1, 0]
var coincidentes =  {"Coincidentes": {$cond: condicion}}
var fase1 = {$project: coincidentes}
var group = {_id: null, SumCoincidentes: {$sum: "$Coincidentes"}}
var fase2 = {$group: group}
var fase3 = {$project: {_id: 0}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3])

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
    return db.pruebas_modificado.aggregate([fase1, fase2, fase3]);
}
comprobar_total("$Statistics.Flights.", ["Cancelled", "Delayed", "Diverted", "On Time"], "$Statistics.Flights.Total");
comprobar_total("$Statistics.Minutes Delayed.", ["Carrier", "Late Aircraft", "National Aviation System", "Security", "Weather"], "$Statistics.Minutes Delayed.Total");

// Minutes Delayed (no todos los valores son correctos), hay que corregirlos
var raiz = "$Statistics.Minutes Delayed."
var array_minutos = [raiz.concat("Carrier"), raiz.concat("Late Aircraft"), raiz.concat("National Aviation System"), raiz.concat("Security"), raiz.concat("Weather")]
var suma = {$sum: array_minutos}
var condicion = {$ne: [suma, "$Statistics.Minutes Delayed.Total"]}
var fase1 = {$match: {$expr: condicion}}
var total = {"Statistics.Minutes Delayed.Total": suma}
var fase2 = {$addFields: total}
var fase3 = {$project: {_id:1, total: "$Statistics.Minutes Delayed.Total"}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3])

// Para corregirlos, mediante un updateMany actualizamos aquellas filas cuyo campo "total" no sea valido
db.pruebas_modificado.aggregate([fase1, fase2, fase3]).forEach(function(id){
    db.pruebas_modificado.updateMany({"_id": id._id}, {$set: {"Statistics.Minutes Delayed.Total": id.total}})
})

// Insertar un nuevo campo, denominado "Total", que calcula el numero total de demoras en los vuelos
var raiz = "$Statistics.# of Delays."
var array_demoras = [raiz.concat("Carrier"), raiz.concat("Late Aircraft"), raiz.concat("National Aviation System"), raiz.concat("Security"), raiz.concat("Weather")]
var suma = {$sum: array_demoras}

var total = {"Statistics.# of Delays.Total": suma}
var fase1 = {$addFields: total}
var fase2 = {$out: "pruebas_modificado"}
db.pruebas_modificado.aggregate([fase1, fase2])

// Ahora, una vez anadidos los campos "Total", podemos hacer varias consultas

// --------------------------------- OPERACIONES CRUD + AGGREGATE -------------------------------------------------
// -----------------------------------------------find()-----------------------------------------------------------
// CONSULTA 1. Consultar el codigo y nombre del aeropuerto con el mayor valor en una categoria, en un mes y anno en particular
function mayor_valor_categoria(mes, anno, categoria) {
    var valor = "$".concat(categoria.toString())
    var query = {"Time.Year": anno, "Time.Month": { $eq: mes}}
    var select = {_id: 0, "Code": 1, "Airport.Code": 1, "Airport.Name": 1, "Total" : valor}
    return db.pruebas_modificado.find(query, select).sort({[categoria] : -1]}).limit(1)
}
// Pruebas
mayor_valor_categoria(1, 2010, "Statistics.Flights.Total")
// Coincidente con el tiroteo producido en el aeropuerto internacional de Los Angeles en noviembre del anno 2013
// URL: https://en.wikipedia.org/wiki/2013_Los_Angeles_International_Airport_shooting
mayor_valor_categoria(11, 2013, "Statistics.# of Delays.Security")

// aggregate()
// CONSULTA 2. Analizar el numero de vuelos realizados en cada anno, entre el anno 2003 y 2016
var subquery = {$sum: "$Statistics.Flights.Total"}
var query = {_id: "$Time.Year", vuelos: subquery}
var fase1 = {$group: query}
var fase2 = {$sort: {"_id": 1}}
db.pruebas_modificado.aggregate([fase1, fase2])

// Para calcular la diferencia de vuelos entre dos años
db.pruebas_modificado.aggregate([fase1, fase2]).forEach(function(doc) {
    var query_aux = {"$gt": doc._id}
    var subfase = {$match: {"Time.Year": query_aux}}
    db.pruebas_modificado.aggregate([subfase, fase1, fase2]).limit(1).forEach(function(doc_aux) {
        var diferencia = doc_aux.vuelos - doc.vuelos
        var texto = ("Diferencia entre " + doc._id + " y " + doc_aux._id).toString()
        print(texto + ": " + diferencia)
    })
})

// CONSULTA 3. Consultar el top 10 aeropuertos con el mayor valor en una categoria
function aeropuerto_mes_anno(categoria) {
    var fase1 = {$group: {_id: null, total: {$sum: "$".concat(categoria)}, aeropuerto: { $push: "$$ROOT" }}}
    var fase2 = {$unwind: "$aeropuerto"}
    var fase3 = {$project: {_id: "$aeropuerto._id", "Airport": "$aeropuerto.Airport", "Time": "$aeropuerto.Time", "Statistics": "$aeropuerto.Statistics", "total": "$total"}}
    var clave = "$".concat(categoria)
    var group = {_id: "$Airport.Name", vuelos: {$sum: clave}, total: {$first: "$total"}}
    var fase4 = {$group: group}
    var fase5 = {$sort: {"vuelos": -1}}
    var porcentaje = {"$multiply": [{"$divide": ["$vuelos", "$total"]}, 100]}
    var field = {"porcentaje (%)": porcentaje}
    var fase6 = {$addFields: field}
    var fase7 = {$project: {"total": 0}}
    return db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7]).limit(10)
}
aeropuerto_mes_anno("Statistics.Flights.Cancelled")
aeropuerto_mes_anno("Statistics.Flights.Delayed")
aeropuerto_mes_anno("Statistics.Flights.On Time")
aeropuerto_mes_anno("Statistics.Flights.Total")

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
db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])

// Si lo agrupamos por meses
var fase8 = {$group: {_id: "$mes", media_cancelaciones: {$sum: "$media_cancelaciones"}}}
var fase9 = {$sort: {"media_cancelaciones": -1}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8, fase9])

// CONSULTA 5. Analizar el numero medio de companias aereas que ha habido a lo largo de los annos
var fase1 = {$group: {_id: "$Time.Year", companias: {$avg: "$Statistics.Carriers.Total"}}}
var fase2 = {$sort: {"_id": 1}}
var fase3 = {$project: {_id: "$_id", companias: {$round: ["$companias", 0]}}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3])

// 5. Consultar companias que desaparecieron entre dos annos consecutivos
var fase1 = {$project: {anno: "$Time.Year" , companias: "$Statistics.Carriers.Names"}}
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
var diferencia = {$setUnion: [{$setDifference: ["$elem0.diferencia", "$elem1.diferencia"]}, {$setDifference: ["$elem1.diferencia", "$elem0.diferencia"]}]}
var fase11 = {$project: {"annos": ["$elem0.anno", "$elem1.anno"] , "diferencia": diferencia}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7, fase8, fase9, fase10, fase11])

// Consultar que compañias que se han ido manteniendo con el transcurso de los annos
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
db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5, fase6, fase7])

// CONSULTA 6. Consultar el aeropuerto con mayor y menor proporcion minutos_demora / total_vuelos
var suma1 = {$sum: "$Statistics.Flights.Total"}
var suma2 = {$sum: "$Statistics.Minutes Delayed.Total"}
var group = {_id: "$Airport.Name", total_vuelos: suma1, total_minutos: suma2}
var fase1 = {$group: group}
var division = {$divide: ["$total_minutos", "$total_vuelos"]}
var fase2 = {$project: {_id: 1, "proporcion": division}}
var fase3 = {$sort: {"proporcion": -1}}
var nuevo_group = {_id: null, mas_demora: {$first: "$$ROOT"}, menos_demora: {$last: "$$ROOT"}}
var fase4 = {$group: nuevo_group}
var fase5 = {$project: {_id: 0}}
db.pruebas_modificado.aggregate([fase1, fase2, fase3, fase4, fase5])
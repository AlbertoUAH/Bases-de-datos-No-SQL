use practica_final

db.codigos_iata.find()

// Lo primero a realizar es modificar el campo coordinates, transformandolo en un array de enteros
var coordenadas = {$split: ["$coordinates", ","]}
var coord0 = { $arrayElemAt: [ coordenadas, 0 ] }
var coord1 = { $arrayElemAt: [ coordenadas, 1 ] }
var elem0 = {$toDouble: {$trim: {input: coord0}}}
var elem1 = {$toDouble: {$trim: {input: coord1}}}
var project = {"position": {"type": "Point", "coordinates": [elem0, elem1]}}
var fase1 = {$addFields: project}
var fase2 = {$out: "codigos_iata"}
db.codigos_iata.aggregate([fase1, fase2])

db.codigos_iata.getIndexes()
db.codigos_iata.createIndex( { position : "2dsphere" } )
db.codigos_iata.find({ position:
   { $near:
   {
     $geometry: { type: "Point",  coordinates: [ -73.9667, 40.78 ] },
            $minDistance: 1000000,
            $maxDistance: 500000000 
   }
  }}
)

db.codigos_iata.aggregate([
   {
     $geoNear: {
        near: { type: "Point", coordinates: [ -73.99279 , 40.719296 ] },
        distanceField: "dist.calculated",
        maxDistance: 100000,
        includeLocs: "dist.location",
        spherical: true
     }
   }
])
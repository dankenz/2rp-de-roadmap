pyspark2 \
    --master yarn \
    --conf spark.ui.port=0 \
    --conf spark.sql.warehouse.dir=/user/${USER}/warehouse


Create Table generation_danielk(
    'generation' int,
    'date_introduced' date)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


Create Table pokemon_danielk(
    'idnum' int, 
    'name' string,  
    'hp' int, 
    'speed' int, 
    'attack' int,
    'special_attack' int, 
    'defense' int, 
    'special_defense' int, 
    'generation' int)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



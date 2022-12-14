import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import datetime

gen = spark.read.csv(path="hdfs:/user/2rp-danielk/.scratchdir/generation.csv", inferSchema=True, header=True,sep=',')
poke = spark.read.csv(path="hdfs:/user/2rp-danielk/.scratchdir/pokemon.csv", inferSchema=True, header=True)
genf=gen.select(F.col('*')).filter(F.col('date_introduced') < '2002-09-28')
genf=genf.cache()
pokemons=poke.join(genf,["generation"],'inner')
pokemons.write.saveAsTable('work_dataeng.pokemons_oldschool_danielk', format="orc", mode="overwrite")








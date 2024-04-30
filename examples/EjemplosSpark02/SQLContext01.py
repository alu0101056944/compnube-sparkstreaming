#
#---------------------------------------SQLContext01.py----------------------------------------

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext



conf = (SparkConf()
         .setMaster("local")
         .setAppName("SQLContext01.py")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf)



sqlContext = SQLContext(sc)

list_p = [('John',19),('Smith',29),('Adam',35),('Henry',50)]
rdd = sc.parallelize(list_p)
ppl = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))

DF_ppl = sqlContext.createDataFrame(ppl) # Se crea el contexto DataFrame

DF_ppl.printSchema()
DF_ppl.show()


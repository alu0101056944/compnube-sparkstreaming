# PySpark Tutorial for Beginners: Machine Learning Example 
# https://www.guru99.com/pyspark-tutorial.html#5
#
#---------------------------------------SQLContext02.py----------------------------------------

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles


conf = (SparkConf()
         .setMaster("local[3]")
         .setAppName("SQLContext02.py")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf)

# Crear el contexto SQL----------------------------------------------------------------------------------

url = "https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv"
sc.addFile(url)
sqlContext = SQLContext(sc)
df = sqlContext.read.csv(SparkFiles.get("adult_data.csv"), header=True, inferSchema= True)  # inferSchema = True para mantener los tipos			

df.printSchema()
df.show(5, truncate = False)			

# Seleccionar columnas ----------------------------------------------------------------------------------
df.select('age','gender').show(20)
# df.select(df['age'], df['gender']).show()

# Filtrar ----------------------------------------------------------------------------------

print "Mayores de cuarenta : %i" % df.filter(df.age > 40).count()


# Contar por Grupos ----------------------------------------------------------------------------------
df.groupBy("education").count().sort("count",ascending=True).show()			

# Describir los Datos ----------------------------------------------------------------------------------
# Permite obtener estadisticas de los datos - describe()
# --> count
# --> media
# --> desviacion standar
# --> min
# --> max

df.describe().show()

# Estadisticas por columnas  ---------------------------------------------------------------------------------
df.describe('capital-gain').show()			


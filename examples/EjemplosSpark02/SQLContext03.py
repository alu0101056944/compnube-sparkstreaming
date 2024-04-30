# PySpark Tutorial for Beginners: Machine Learning Example 
# https://www.guru99.com/pyspark-tutorial.html#5
#
#---------------------------------------SQLContext03.py----------------------------------------

from pyspark.sql import SparkSession



# Crear el contexto SQL----------------------------------------------------------------------------------

session = SparkSession.builder.appName("SQLContex03").master("local[2]").getOrCreate()
dataFrameReader = session.read # Retorna un Objeto reader para DataFrames
# Se utiliza para cargar objetos externos

df = dataFrameReader \
      .option("header", "true") \
      .option("inferSchema", value = True) \
      .csv("in/adult_data.csv")



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

df.stop()

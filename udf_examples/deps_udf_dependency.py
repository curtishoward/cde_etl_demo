from demo_utils import deps_udf
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/cde_examples.ini')
prefix = config['CDE-examples']['userPrefix'].replace('"','').replace("\'",'')

if __name__ == '__main__':
  spark = SparkSession.builder.appName("Python UDF penv-dependency example").getOrCreate() 
  spark.udf.register("OFFSET_FROM_ZIP", deps_udf.tzOffsetFromZip)
  
  spark.sql(f"""SELECT OFFSET_FROM_ZIP(zip) AS offset_from_utc, zip FROM 
                (
                   SELECT zip FROM {prefix}_factory.experimental_motors_enriched LIMIT 10
                )""").show()

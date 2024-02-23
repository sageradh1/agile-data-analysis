from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,sum


def mariaDB(spark): 

    
    server = "localhost"
    port = 3306
    database = "citytemperature"
    url = f"jdbc:mysql://{server}:{port}/{database}?permitMysqlScheme"
    
    properties = {
        "user": "root",
        "password": "",
        "driver": "org.mariadb.jdbc.Driver"
    }
    
    table_name = "city_temperature"
    
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .load()

  
    temperature_df = spark.read.jdbc(url, "(select * from city_temperature ) tab", properties=properties)
   
    return temperature_df



if __name__ == "__main__" :
    spark = SparkSession.builder \
        .appName("MariaDBReadExample") \
        .config("spark.jars", "/home/azureuser/mariadbdriver/mariadb-java-client-3.3.2.jar") \
        .getOrCreate()

    temperature_df = mariaDB(spark)
    print('DataSource 2(MariaDB):')
    temperature_df.show()

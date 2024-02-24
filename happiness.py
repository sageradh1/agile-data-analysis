from pyspark.sql import SparkSession
from pyspark.sql.functions import  sum ,count
from pyspark import SparkContext

def datasource1(spark):
    happiness_df = spark.read.csv("Happines_dataset.csv", header=True)
    

    avg_happiness_by_country = happiness_df.groupBy('Country').agg({'Happiness Score': 'avg'})

    return avg_happiness_by_country

if __name__ == "__main__" :
    spark = SparkSession.builder.appName("HappinessIndex").getOrCreate()
    avg_happiness_by_country = datasource1(spark)
    print("Happiness Index(CSV):")
    avg_happiness_by_country.show()
    spark.stop()


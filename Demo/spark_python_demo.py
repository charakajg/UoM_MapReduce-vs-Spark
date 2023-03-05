from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()

df = spark.read.csv("s3a://bda-test-data-1/demo/input/DelayedFlights-updated.csv")
df.count()

from pyspark.sql.types import StructType, StringType, IntegerType
schema = StructType() \
  .add("ID",IntegerType(),True) \
  .add("Year",IntegerType(),True) \
  .add("Month",IntegerType(),True) \
  .add("DayofMonth",IntegerType(),True) \
  .add("DayOfWeek",IntegerType(),True) \
  .add("DepTime",IntegerType(),True) \
  .add("CRSDepTime",IntegerType(),True) \
  .add("ArrTime",IntegerType(),True) \
  .add("CRSArrTime",IntegerType(),True) \
  .add("UniqueCarrier",StringType(),True) \
  .add("FlightNum",StringType(),True) \
  .add("TailNum",StringType(),True) \
  .add("ActualElapsedTime",IntegerType(),True) \
  .add("CRSElapsedTime",IntegerType(),True) \
  .add("AirTime",IntegerType(),True) \
  .add("ArrDelay",IntegerType(),True) \
  .add("DepDelay",IntegerType(),True) \
  .add("Origin",StringType(),True) \
  .add("Dest",StringType(),True) \
  .add("Distance",IntegerType(),True) \
  .add("TaxiIn",IntegerType(),True) \
  .add("TaxiOut",IntegerType(),True) \
  .add("Cancelled",IntegerType(),True) \
  .add("CancellationCode",StringType(),True) \
  .add("Diverted",IntegerType(),True) \
  .add("CarrierDelay",IntegerType(),True) \
  .add("WeatherDelay",IntegerType(),True) \
  .add("NASDelay",IntegerType(),True) \
  .add("SecurityDelay",IntegerType(),True) \
  .add("LateAircraftDelay",IntegerType(),True)

flightdelays_df = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("s3a://bda-test-data-1/demo/input/DelayedFlights-updated.csv")

flightdelays_df.count()
flightdelays_df.head()

output_df = flightdelays_df.groupby("Year").avg("ArrDelay")
output_df.show()
output_df.write.mode("overwrite").csv("s3a://bda-test-data-1/demo/spark_output/average_arrival_delay_by_year")

flightdelays_df.groupby("Year").sum("CarrierDelay").show()
flightdelays_df.groupby("Year").sum("NASDelay").show()
flightdelays_df.groupby("Year").sum("WeatherDelay").show()
flightdelays_df.groupby("Year").sum("LateAircraftDelay").show()
flightdelays_df.groupby("Year").sum("SecurityDelay").show()


spark.stop()

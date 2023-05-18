from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime
import math



def to_hours_ago(timestamp):
    current_dt = datetime.utcnow()
    timestamp = timestamp[:-3]
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    hours_ago = int((current_dt - dt).total_seconds() // 3600)
    return hours_ago

def calculate_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Radius of the Earth in kilometers
    radius = 6371.0

    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = radius * c

    return distance

def in_area_count(traffy_road_prep,rows, r) :
    road_rows = traffy_road_prep.collect()
    count = []
    i=0
    for r_row in road_rows:
        count.append([r_row.ID,0])
        r_lat = float(r_row.Latitude)
        r_lon = float(r_row.Longitude)
        for row in rows:
            lat = float(row.Latitude)
            lon = float(row.Longitude)
            if calculate_distance(r_lat,r_lon,lat,lon) < r:
                count[i][1] += 1
        i+=1
    return count


def preprocess_road(traffy_road_path,spark):
    traffy_road_prep  = spark.read.option("delimiter", ",").option("header", True,).option("multiline", True).option("encoding", "UTF-8").csv(traffy_road_path)
    traffy_road_prep = traffy_road_prep.filter(traffy_road_prep.state == "รอรับเรื่อง")
    traffy_road_prep = traffy_road_prep.select(["ticket_id", "address", "timestamp", "coords"])
    getLatitudeFromCord = udf(lambda x:x[1:-1].split(',')[1].strip()[1:-1],StringType()) 
    getLongitudeFromCord = udf(lambda x:x[1:-1].split(',')[0].strip()[1:-1],StringType())
    udf_to_days_ago = udf(to_hours_ago, IntegerType())
    traffy_road_prep = traffy_road_prep.withColumn('Latitude', getLatitudeFromCord(col('coords')))
    traffy_road_prep = traffy_road_prep.withColumn('Longitude', getLongitudeFromCord(col('coords')))
    traffy_road_prep = traffy_road_prep.drop("coords")
    traffy_road_prep = traffy_road_prep.withColumn('hours_ago', udf_to_days_ago(col('timestamp')))
    traffy_road_prep = traffy_road_prep.drop("timestamp")
    traffy_road_prep = traffy_road_prep.withColumn("ID", monotonically_increasing_id())
    traffy_road_prep = traffy_road_prep.filter((traffy_road_prep.Latitude >= 13.548) & (traffy_road_prep.Latitude <= 14.1) & 
                            (traffy_road_prep.Longitude >= 100.299) & (traffy_road_prep.Longitude <= 100.91))
    traffy_road_prep = traffy_road_prep.filter(~(traffy_road_prep.ticket_id.isNull()) & (traffy_road_prep.ticket_id != ""))

    return traffy_road_prep

def preprocess_traffic(traffic_path,spark):
    traffic_prep  = spark.read.option("delimiter", ",").option("header", True,).option("multiline", True).option("encoding", "UTF-8").csv(traffic_path)
    Dict_Null = {col:traffic_prep.filter(traffic_prep[col].isNull()).count() for col in traffic_prep.columns}
    traffic_prep = traffic_prep.select(["District", "Road", "latitude", "longitude"])
    traffic_prep = traffic_prep.withColumnRenamed('latitude', 'Latitude')
    traffic_prep = traffic_prep.withColumnRenamed('longitude', 'Longitude')

    return traffic_prep

def preprocess_accident(accident_path,spark):
    accident_prep = spark.read.option("delimiter", ",").option("header", True,).option("multiline", True).option("encoding", "UTF-8").csv(accident_path)
    accident_prep = accident_prep.select(["รายละเอียด", "สำนักงานเขต", "พิกัด"])
    getLatitude = udf(lambda x:x.split(',')[0].strip(),StringType()) 
    getLongitude = udf(lambda x:x.split(',')[1].strip(),StringType())
    accident_prep = accident_prep.withColumn('Latitude', getLatitude(col('พิกัด')))
    accident_prep = accident_prep.withColumn('Longitude', getLongitude(col('พิกัด')))
    accident_prep = accident_prep.withColumnRenamed('รายละเอียด', 'Location')
    accident_prep = accident_prep.withColumnRenamed('สำนักงานเขต', 'District')
    accident_prep = accident_prep.drop("พิกัด")

    return accident_prep


def preprocess_data():

    spark_url = 'local'

    spark = SparkSession.builder\
            .master(spark_url)\
            .appName('Spark ML')\
            .getOrCreate()

    accident_path = "./accident_data.csv"
    traffic_path = "./traffic.csv"
    traffy_road_path = "./road.csv"

    traffy_road_prep = preprocess_road(traffy_road_path,spark)
    traffic_prep = preprocess_traffic(traffic_path,spark)
    accident_prep = preprocess_accident(accident_path,spark)

    trafficCount = in_area_count(traffy_road_prep,traffic_prep.collect(), 2)
    accidentCount = in_area_count(traffy_road_prep,accident_prep.collect(), 2)

    list_df = spark.createDataFrame(trafficCount, ["ID", "trafficCount"])
    traffy_road_prep = traffy_road_prep.join(list_df, on=["ID"], how="left")

    list_df = spark.createDataFrame(accidentCount, ["ID", "accidentCount"])
    traffy_road_prep = traffy_road_prep.join(list_df, on=["ID"], how="left")


    return traffy_road_prep
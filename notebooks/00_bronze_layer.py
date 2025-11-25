import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


######## Ingestion of Bookings data ############

@dlt.table(
    name = 'bronze_bookings_incremental',
    comment = 'Airline Booking Data'
)

def bronze_bookings_incremental():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option('cloudFiles.includeExistingFiles', 'true')\
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
        .option('header', 'true')\
        .load('/Volumes/aviation/airline_data/source_data/bookings/')
    
    return df



######## Ingestion of Airports data ############

@dlt.table(
    name = 'bronze_airports_incremental',
    comment = 'Airline Airport Data'
)

def bronze_airports_incremental():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option('cloudFiles.includeExistingFiles', 'true')\
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
        .option('header', 'true')\
        .load('/Volumes/aviation/airline_data/source_data/airports/')
    
    return df



######## Ingestion of Flights data ############

@dlt.table(
    name = 'bronze_flights_incremental',
    comment = 'Airline Flights Data'
)

def bronze_flights_incremental():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option('cloudFiles.includeExistingFiles', 'true')\
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
        .option('header', 'true')\
        .load('/Volumes/aviation/airline_data/source_data/flights/')
    
    return df



######## Ingestion of Passenegers data ############

@dlt.table(
    name = 'bronze_passengers_incremental',
    comment = 'Airline Passengers Data'
)

def bronze_passengers_incremental():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option('cloudFiles.includeExistingFiles', 'true')\
        .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
        .option('header', 'true')\
        .load('/Volumes/aviation/airline_data/source_data/passengers/')
    
    return df
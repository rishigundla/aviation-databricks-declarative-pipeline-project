import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


######## Cleaning & Transforming Bookings data ############

@dlt.table(
    name = 'silver_bookings_cleansed',
    comment = 'Airline Booking Data Cleansed'
)

@dlt.expect_all_or_fail({
    "valid_booking_id": "booking_id IS NOT NULL",
    "valid_passenger_id": "passenger_id IS NOT NULL",
    "valid_flight_id": "flight_id IS NOT NULL",
    "valid_airport_id": "airport_id IS NOT NULL"
})
@dlt.expect("valid_amount", "amount IS NOT NULL")
@dlt.expect_or_drop("valid_booking_date", "booking_date IS NOT NULL")

def silver_bookings_cleansed():
    df = dlt.read_stream('bronze_bookings_incremental')
    df = df.withColumn('amount', col('amount').cast('double'))
    df = df.withColumn('booking_date',to_date(col('booking_date')))
    df = df.withColumn('modified_date',current_timestamp())
    
    return df



######## Cleaning & Transforming Airports data ############

@dlt.table(
    name = 'silver_airports_cleansed',
    comment = 'Airline Airport Data Cleansed'
)

@dlt.expect_or_fail("valid_airport_id", "airport_id IS NOT NULL")
@dlt.expect("valid_airport_name", "airport_name IS NOT NULL")
@dlt.expect("valid_city", "city IS NOT NULL")
@dlt.expect("valid_country", "country IS NOT NULL")

def silver_airports_cleansed():
    df = dlt.read_stream('bronze_airports_incremental')
    df = df.withColumn('modified_date',current_timestamp())

    return df



######## Cleaning & Transforming Flights data ############

@dlt.table(
    name = 'silver_flights_cleansed',
    comment = 'Airline Flights Data Cleansed'
)

@dlt.expect_all_or_fail({
    "valid_flight_id": "flight_id IS NOT NULL",
    "valid_flight_date": "flight_date IS NOT NULL"
})
@dlt.expect("valid_airline", "airline IS NOT NULL")
@dlt.expect("valid_origin", "origin IS NOT NULL")
@dlt.expect("valid_destination", "destination IS NOT NULL")


def silver_flights_cleansed():
    df = dlt.read_stream('bronze_flights_incremental')
    df = df.withColumn('flight_date',to_date(col('flight_date')))
    df = df.withColumn('modified_date',current_timestamp())
    
    return df



######## Cleaning & Transforming Passenegers data ############

@dlt.table(
    name = 'silver_passengers_cleansed',
    comment = 'Airline Passengers Data Cleansed'
)

@dlt.expect_or_fail("valid_passenger_id", "passenger_id IS NOT NULL")
@dlt.expect("valid_passenger_name", "name IS NOT NULL")
@dlt.expect("valid_passenger_nationality", "nationality IS NOT NULL")
@dlt.expect("valid_passenger_gender", "gender IS NOT NULL")

def silver_passengers_cleansed():
    df = dlt.read_stream('bronze_passengers_incremental')
    df = df.withColumn('modified_date',current_timestamp())
    
    return df
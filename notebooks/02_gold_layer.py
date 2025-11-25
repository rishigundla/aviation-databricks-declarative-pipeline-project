import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


############ Airline Business Joined Table #######################

@dlt.table(
  name = 'airline_details',
  comment = "Detail table for airline business"
)
def airline_details():
    bookings = dlt.read('scd2_bookings')
    aiports = dlt.read('scd2_airports')
    flights = dlt.read('scd2_flights')
    passengers = dlt.read('scd2_passengers')

    joined = bookings.join(aiports, bookings.airport_id == aiports.airport_id, 'inner') \
                     .join(flights, bookings.flight_id == flights.flight_id, 'inner') \
                     .join(passengers, bookings.passenger_id == passengers.passenger_id, 'inner') \
                     .where((bookings.__END_AT.isNull()) & (aiports.__END_AT.isNull()) & (flights.__END_AT.isNull()) & (passengers.__END_AT.isNull())) \
                     .select(bookings.booking_id, bookings.airport_id, bookings.flight_id, bookings.passenger_id, bookings.amount, bookings.booking_date, 
                             aiports.airport_name, aiports.city, aiports.country, 
                             flights.airline, flights.origin, flights.destination, flights.flight_date,
                             passengers.name, passengers.gender, passengers.nationality)
    
    return joined


############ Airline Business Agggregated Table #######################

@dlt.table(
  name = 'airline_agg',
  comment = "Aggregated table for airline business"
)
def airline_agg():
    airline_details = dlt.read('airline_details')

    airline_agg = airline_details.groupBy('airline', 'origin', 'destination').agg(
        count('booking_id').alias('total_bookings'),
        count('passenger_id').alias('total_passengers'),
        count('flight_id').alias('total_flights'),
        round(sum('amount'),1).alias('total_amount'), 
        round(avg('amount'),1).alias('avg_amount')
        )
    
    return airline_agg

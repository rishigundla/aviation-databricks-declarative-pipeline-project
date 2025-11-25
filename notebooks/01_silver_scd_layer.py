import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


######## Bookings Data - SCD2 with Auto CDC ############

dlt.create_streaming_table('scd2_bookings')
dlt.create_auto_cdc_flow(
    target='scd2_bookings',
    source='silver_bookings_cleansed',
    keys=['booking_id'],
    sequence_by=col('modified_date'),
    stored_as_scd_type=2,
    except_column_list=['modified_date']
)

######## Airports Data with SCD2 with Auto CDC ############

dlt.create_streaming_table('scd2_airports')
dlt.create_auto_cdc_flow(
    target='scd2_airports',
    source='silver_airports_cleansed',
    keys=['airport_id'],
    sequence_by=col('modified_date'),
    stored_as_scd_type=2,
    except_column_list=['modified_date']
)



######## Flights data with SCD2 with Auto CDC ############

dlt.create_streaming_table('scd2_flights')
dlt.create_auto_cdc_flow(
    target='scd2_flights',
    source='silver_flights_cleansed',
    keys=['flight_id'],
    sequence_by=col('modified_date'),
    stored_as_scd_type=2,
    except_column_list=['modified_date']
)


######## Passenegers Data SCD2 with Auto CDC ############

dlt.create_streaming_table('scd2_passengers')
dlt.create_auto_cdc_flow(
    target='scd2_passengers',
    source='silver_passengers_cleansed',
    keys=['passenger_id'],
    sequence_by=col('modified_date'),
    stored_as_scd_type=2,
    except_column_list=['modified_date']
)


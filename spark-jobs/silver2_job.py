"""
Silver 2: Data standardization via SQL.

Reads from Silver 1's cleaned Delta table, registers a temp view 'silver1',
and runs SILVER2_TRANSFORM_SQL for data standardization.
"""

import os

from common import build_spark, wait_for_delta_table

SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/silver2"
)

# SQL for Silver 2 (Data Standardization).
# Modify this query to apply standardization rules across sources.
SILVER2_TRANSFORM_SQL = """
SELECT
    record_id,
    batch_id,
    table_name,
    ingestion_ts,
    topic,
    kafka_timestamp,
    crash_record_id,

    CASE
        WHEN crash_date LIKE '____-__-__T%' 
            THEN SUBSTRING(crash_date, 1, 10)
        WHEN crash_date LIKE '____/__/__%' 
            THEN REPLACE(SUBSTRING(crash_date, 1, 10), '/', '-')
        WHEN crash_date LIKE '____-__-__' 
            THEN crash_date
        WHEN crash_date LIKE '__/__/____' 
            THEN SUBSTRING(crash_date, 7, 4) || '-' || SUBSTRING(crash_date, 4, 2) || '-' || SUBSTRING(crash_date, 1, 2)
        WHEN crash_date LIKE '__/__/____ %' 
            THEN SUBSTRING(crash_date, 7, 4) || '-' || SUBSTRING(crash_date, 1, 2) || '-' || SUBSTRING(crash_date, 4, 2)
        ELSE crash_date
    END AS crash_date,

    posted_speed_limit,
    traffic_control_device,
    device_condition,

    CASE 
        WHEN weather_condition IN ('CLEAR', 'C', 'Clear', 'Clear Weather') 
            THEN 'CLEAR'
        WHEN weather_condition IN ('RAIN', 'R', 'Rain', 'Rainy') 
            THEN 'RAIN'
        WHEN weather_condition IN ('UNKNOWN', 'U', 'Unknown', 'Not Known') 
            THEN 'UNKNOWN'
        WHEN weather_condition IN ('CLOUDY/OVERCAST', 'O', 'Overcast', 'Cloudy') 
            THEN 'CLOUDY'
        ELSE weather_condition
    END AS weather_condition,

    CASE 
        WHEN lighting_condition IN ('DAYLIGHT', 'D', 'Daylight', 'Morning', 'Day') 
            THEN 'DAYLIGHT'
        WHEN lighting_condition IN ('DARKNESS, LIGHTED ROAD', 'N, L', 'Darkness, Lighted Road', 'Night, Lighted', 'Night with Light') 
            THEN 'DARKNESS, LIGHTED ROAD'
        WHEN lighting_condition IN ('DARKNESS', 'N', 'Darkness', 'Night') 
            THEN 'DARKNESS'
        WHEN lighting_condition IN ('UNKNOWN', 'U', 'Not Known') 
            THEN 'UNKNOWN'
        ELSE lighting_condition
    END AS lighting_condition,

    first_crash_type,
    trafficway_type,
    alignment,
    roadway_surface_cond,
    road_defect,
    report_type,
    crash_type,
    private_property_i,

    CASE 
        WHEN hit_and_run_i IN ('Y', 'TRUE', 'Yes', 'right') 
            THEN 'Y'
        WHEN hit_and_run_i IN ('N', 'FALSE', 'No', 'wrong') 
            THEN 'N'
        ELSE NULL
    END AS hit_and_run_i,

    CASE 
        WHEN damage IN ('OVER $1,500', '> $1500', 'HIGH') 
            THEN 'OVER $1,500'
        WHEN damage IN ('$501 - $1,500', '$501 - $1500', 'MEDIUM') 
            THEN '$501 - $1,500'
        WHEN damage IN ('$500 OR LESS', '<= $500', '≤ $500', 'LOW') 
            THEN '$500 OR LESS'
        ELSE damage
    END AS damage,

    date_police_notified,
    prim_contributory_cause,
    sec_contributory_cause,
    street_no,

    CASE 
        WHEN street_direction IN ('S', 'South', 'SOU') THEN 'S'
        WHEN street_direction IN ('N', 'North', 'NOR') THEN 'N'
        WHEN street_direction IN ('E', 'East', 'EAS')  THEN 'E'
        WHEN street_direction IN ('W', 'West', 'WES')  THEN 'W'
        ELSE street_direction
    END AS street_direction,

    street_name,
    beat_of_occurrence,

    CASE 
        WHEN num_units IN ('1', '1.0', 'one', '1 units')   THEN 1.0
        WHEN num_units IN ('2', '2.0', 'two', '2 units')   THEN 2.0
        WHEN num_units IN ('3', '3.0', 'three', '3 units') THEN 3.0
        WHEN num_units IN ('4', '4.0', 'four', '4 units')  THEN 4.0
        WHEN num_units IN ('5', '5.0', 'five', '5 units')  THEN 5.0
        WHEN num_units IN ('6', '6.0', 'six', '6 units')   THEN 6.0
        ELSE CAST(num_units AS DECIMAL(10,1))
    END AS num_units,

    CASE 
        WHEN crash_month IN ('1', 'January', 'Jan', 'Ja')   THEN 1
        WHEN crash_month IN ('2', 'February', 'Feb', 'Fe')  THEN 2
        WHEN crash_month IN ('3', 'March', 'Mar', 'Ma')     THEN 3
        WHEN crash_month IN ('4', 'April', 'Apr', 'Ap')     THEN 4
        WHEN crash_month IN ('5', 'May', 'My')              THEN 5
        WHEN crash_month IN ('6', 'June', 'Jun', 'Ju')      THEN 6
        WHEN crash_month IN ('7', 'July', 'Jul', 'Jl')      THEN 7
        WHEN crash_month IN ('8', 'August', 'Aug', 'Au')    THEN 8
        WHEN crash_month IN ('9', 'September', 'Sep', 'Se') THEN 9
        WHEN crash_month IN ('10', 'October', 'Oct', 'Oc')  THEN 10
        WHEN crash_month IN ('11', 'November', 'Nov', 'No') THEN 11
        WHEN crash_month IN ('12', 'December', 'Dec', 'De') THEN 12
        ELSE CAST(crash_month AS INTEGER)
    END AS crash_month,

    most_severe_injury,
    injuries_total,
    injuries_fatal,
    injuries_incapacitating,
    injuries_non_incapacitating,
    injuries_reported_not_evident,
    injuries_no_indication,
    injuries_unknown,

    CASE 
        WHEN crash_hour IN ('0', '00:00', '12 AM')              THEN 0
        WHEN crash_hour IN ('1', '01:00', '1 AM', '01 AM')      THEN 1
        WHEN crash_hour IN ('2', '02:00', '2 AM', '02 AM')      THEN 2
        WHEN crash_hour IN ('3', '03:00', '3 AM', '03 AM')      THEN 3
        WHEN crash_hour IN ('4', '04:00', '4 AM', '04 AM')      THEN 4
        WHEN crash_hour IN ('5', '05:00', '5 AM', '05 AM')      THEN 5
        WHEN crash_hour IN ('6', '06:00', '6 AM', '06 AM')      THEN 6
        WHEN crash_hour IN ('7', '07:00', '7 AM', '07 AM')      THEN 7
        WHEN crash_hour IN ('8', '08:00', '8 AM', '08 AM')      THEN 8
        WHEN crash_hour IN ('9', '09:00', '9 AM', '09 AM')      THEN 9
        WHEN crash_hour IN ('10', '10:00', '10 AM')             THEN 10
        WHEN crash_hour IN ('11', '11:00', '11 AM')             THEN 11
        WHEN crash_hour IN ('12', '12:00', '12 PM')             THEN 12
        WHEN crash_hour IN ('13', '13:00', '1 PM', '01 PM')     THEN 13
        WHEN crash_hour IN ('14', '14:00', '2 PM', '02 PM')     THEN 14
        WHEN crash_hour IN ('15', '15:00', '3 PM', '03 PM')     THEN 15
        WHEN crash_hour IN ('16', '16:00', '4 PM', '04 PM')     THEN 16
        WHEN crash_hour IN ('17', '17:00', '5 PM', '05 PM')     THEN 17
        WHEN crash_hour IN ('18', '18:00', '6 PM', '06 PM')     THEN 18
        WHEN crash_hour IN ('19', '19:00', '7 PM', '07 PM')     THEN 19
        WHEN crash_hour IN ('20', '20:00', '8 PM', '08 PM')     THEN 20
        WHEN crash_hour IN ('21', '21:00', '9 PM', '09 PM')     THEN 21
        WHEN crash_hour IN ('22', '22:00', '10 PM')             THEN 22
        WHEN crash_hour IN ('23', '23:00', '11 PM')             THEN 23
        ELSE CAST(crash_hour AS INTEGER)
    END AS crash_hour,

    CASE 
        WHEN crash_day_of_week IN ('1', 'Sunday', 'Sun', 'Su')       THEN 1
        WHEN crash_day_of_week IN ('2', 'Monday', 'Mon', 'Mo')       THEN 2
        WHEN crash_day_of_week IN ('3', 'Tuesday', 'Tue', 'Tu')      THEN 3
        WHEN crash_day_of_week IN ('4', 'Wednesday', 'Wed', 'We')    THEN 4
        WHEN crash_day_of_week IN ('5', 'Thursday', 'Thu', 'Th')     THEN 5
        WHEN crash_day_of_week IN ('6', 'Friday', 'Fri', 'Fr')       THEN 6
        WHEN crash_day_of_week IN ('7', 'Saturday', 'Sat', 'Sa')     THEN 7
        ELSE CAST(crash_day_of_week AS INTEGER)
    END AS crash_day_of_week,

    idot_control_no,
    latitude,
    longitude,
    location,
    intersection_related_i,
    crash_date_est_i,
    photos_taken_i,
    statements_taken_i,
    work_zone_i,
    work_zone_type,
    lane_cnt,
    workers_present_i,
    dooring_i
FROM silver1
"""


def main():
    spark = build_spark("Silver2StandardizeTransform")

    wait_for_delta_table(SILVER1_PATH)

    silver1_stream = spark.readStream.format("delta").load(SILVER1_PATH)
    silver1_stream.createOrReplaceTempView("silver1")

    silver2 = spark.sql(SILVER2_TRANSFORM_SQL)

    query = (
        silver2.writeStream.format("delta")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER2_PATH)
    )

    print(f"[silver2] Streaming {SILVER1_PATH} -> {SILVER2_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

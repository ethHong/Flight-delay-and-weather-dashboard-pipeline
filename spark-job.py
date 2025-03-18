from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    to_timestamp,
    date_trunc,
    row_number,
    split,
    regexp_extract,
    regexp_replace,
    when,
    round,
    monotonically_increasing_id,
    broadcast,
    floor,
    lpad,
    concat_ws,
    lit,
    to_utc_timestamp,
    unix_timestamp,
    from_unixtime,
    from_utc_timestamp,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import os


def main():

    spark = SparkSession.builder.appName("405_project").getOrCreate()

    airline_df = (
        spark.read.option(
            "spark.sql.files.maxPartitionBytes", "128MB"
        )  # Optimize partition size
        .csv("airline_delay_cancellation_data.csv", header=True, inferSchema=True)
        .select(
            col("FL_DATE"),
            col("OP_CARRIER"),
            col("OP_CARRIER_FL_NUM"),
            col("ORIGIN"),
            col("DEST"),
            col("CRS_DEP_TIME"),
            col("DEP_TIME"),
            col("DEP_DELAY"),
            col("CRS_ARR_TIME"),
            col("ARR_TIME"),
            col("ARR_DELAY"),
            col("CANCELLED"),
            col("DIVERTED"),
            col("ACTUAL_ELAPSED_TIME"),
            col("TIME_ZONE_ORIGIN"),
            col("TIME_ZONE_DEST"),
            col("CITY_ORIGIN"),
            col("CITY_DEST"),
            col("STATE_ORIGIN"),
            col("STATE_DEST"),
            col("COUNTRY_ORIGIN"),
            col("COUNTRY_DEST"),
            col("LONGITUDE_ORIGIN"),
            col("LONGITUDE_DEST"),
            col("LATITUDE_ORIGIN"),
            col("LATITUDE_DEST"),
            col("OP_CARRIER_NAME"),
            col("AIRPORT_NAME_ORIGIN"),
            col("AIRPORT_NAME_DEST"),
        )
    )

    # Remove Unknown timezone (Added by Ethan)
    ##
    airline_df = airline_df.withColumn(
        "TIME_ZONE_ORIGIN",
        when(col("TIME_ZONE_ORIGIN") == "Unknown", None).otherwise(
            col("TIME_ZONE_ORIGIN")
        ),
    )

    airline_df = airline_df.withColumn(
        "TIME_ZONE_DEST",
        when(col("TIME_ZONE_DEST") == "Unknown", None).otherwise(col("TIME_ZONE_DEST")),
    )
    ##

    # Extract hour and minute from DEP_TIME
    airline_df = airline_df.withColumn("Hour", floor(col("DEP_TIME") / 100).cast("int"))

    # Format Hour and Minute as two-digit strings
    airline_df = airline_df.withColumn("Hour", lpad(col("Hour"), 2, "0"))

    # Create Departure_Time in local timezone (based on the Timezone column)
    airline_df = airline_df.withColumn(
        "Departure_Time_Local",
        to_timestamp(
            concat_ws(
                " ", col("FL_DATE"), concat_ws(":", col("Hour"), lit("00"), lit("00"))
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    # Convert Departure_Time_Local to UTC based on the Timezone column
    airline_df = airline_df.withColumn(
        "Departure_Time_UTC",
        to_utc_timestamp(col("Departure_Time_Local"), col("TIME_ZONE_ORIGIN")),
    )

    # Drop columns
    airline_df = airline_df.drop("Hour", "Minute")

    airline_df = airline_df.withColumn(
        "Arrival_Time_UTC",
        from_unixtime(
            unix_timestamp(col("Departure_Time_UTC"))
            + (round(col("ACTUAL_ELAPSED_TIME") / 60) * 3600)
        ),
    )

    airline_df = airline_df.withColumn(
        "Arrival_Time_Local",
        from_utc_timestamp(col("Arrival_Time_UTC"), col("TIME_ZONE_DEST")),
    )

    airline_df = airline_df.withColumn(
        "Departure_Delay_Category",
        when(col("DEP_DELAY") < 0, "Early_Departure")
        .when((col("DEP_DELAY") >= 0) & (col("DEP_DELAY") <= 15), "On-Time")
        .when((col("DEP_DELAY") > 15) & (col("DEP_DELAY") <= 30), "Minor Delay")
        .when((col("DEP_DELAY") > 30) & (col("DEP_DELAY") <= 60), "Moderate Delay")
        .when((col("DEP_DELAY") > 60) & (col("DEP_DELAY") <= 120), "Severe Delay")
        .otherwise("Extreme Delay"),
    )

    airline_df = airline_df.withColumn(
        "Arrival_Delay_Category",
        when(col("ARR_DELAY") < 0, "Early_Arrival")
        .when((col("ARR_DELAY") >= 0) & (col("ARR_DELAY") <= 15), "On-Time")
        .when((col("ARR_DELAY") > 15) & (col("ARR_DELAY") <= 30), "Minor Delay")
        .when((col("ARR_DELAY") > 30) & (col("ARR_DELAY") <= 60), "Moderate Delay")
        .when((col("ARR_DELAY") > 60) & (col("ARR_DELAY") <= 120), "Severe Delay")
        .otherwise("Extreme Delay"),
    )

    weather_df = (
        spark.read.option(
            "spark.sql.files.maxPartitionBytes", "128MB"
        )  # should save RAM
        .csv("merged_weather.csv", header=True, inferSchema=True)
        .select(
            col("time"),
            col("temp").alias("temperature_deg_c"),
            col("prcp").alias("precipitation"),
            col("rhum").alias("relative_humidity"),
            col("wspd").alias("wind_speed_km_per_hr"),
            col("airport"),
            col("coco"),
        )
    )

    weather_df = weather_df.withColumn(
        "weather_condition",
        when(
            (col("precipitation") == 0)
            & (col("temperature_deg_c").between(-2, 35.0))
            & (col("relative_humidity") < 50)
            & (col("wind_speed_km_per_hr") < 20),
            "Clear",
        )
        .when(
            (col("precipitation") == 0)
            & (col("temperature_deg_c").between(-10.0, 45.0))
            & (col("relative_humidity").between(50, 75))
            & (col("wind_speed_km_per_hr") < 30),
            "Partly Cloudy",
        )
        .when(
            (col("precipitation") == 0)
            & (col("temperature_deg_c").between(-10.0, 45.0))
            & (col("relative_humidity") > 75),
            "Cloudy",
        )
        .when(
            (col("precipitation") > 0.01)
            & (col("precipitation") <= 10.0)
            & (col("temperature_deg_c").between(2, 45.0))
            & (col("relative_humidity") > 55)
            & (col("wind_speed_km_per_hr") < 25),
            "Light Rain",
        )
        .when(
            (col("precipitation") > 10.0)
            & (col("temperature_deg_c").between(2, 45.0))
            & (col("relative_humidity") > 65)
            & (col("wind_speed_km_per_hr") < 25),
            "Heavy Rain",
        )
        .when(
            (col("precipitation") > 0.05)
            & (col("temperature_deg_c").between(-20, 2))
            & (col("relative_humidity") > 55),
            "Snow",
        )
        .when(
            (col("precipitation") > 2.0)
            & (col("temperature_deg_c").between(-30, 2))
            & (col("relative_humidity") > 60),
            "Heavy Snow",
        )
        .when(
            (col("wind_speed_km_per_hr").between(25, 35))
            & (col("precipitation") > 1)
            & (col("relative_humidity") > 55),
            "Windy Rain",
        )
        .when(
            (col("wind_speed_km_per_hr").between(35, 40))
            & (col("temperature_deg_c") > -5)
            & (col("precipitation") > 2)
            & (col("relative_humidity") > 60),
            "Stormy",
        )
        .when(
            (col("temperature_deg_c") < -5)
            & (col("wind_speed_km_per_hr").between(40, 50))
            & (col("precipitation") > 2)
            & (col("relative_humidity") > 60),
            "Blizzard",
        )
        .when(
            (col("wind_speed_km_per_hr") > 50)
            & (col("precipitation") > 5)
            & (col("relative_humidity") > 70),
            "Severe Storm",
        )
        .when(
            (col("temperature_deg_c") > 35)
            & (col("relative_humidity") < 30)
            & (col("wind_speed_km_per_hr") < 20),
            "Hot and Dry",
        )
        .when(
            (col("temperature_deg_c") < -15) & (col("wind_speed_km_per_hr") < 20),
            "Extreme Cold",
        )
        .otherwise("Unknown"),
    )

    weather_df = weather_df.withColumn(
        "weather_condition",
        when(col("coco").isin(1, 2), "Clear")
        .when(col("coco").isin(3, 4, 5, 6), "Cloudy")
        .when(col("coco").isin(7, 8, 17), "Light Rain")
        .when(col("coco").isin(9, 10, 11, 18), "Heavy Rain")
        .when(col("coco").isin(12, 14, 15, 19, 21, 24), "Snow")
        .when(col("coco").isin(13, 16, 20, 22), "Heavy Snow")
        .when(col("coco").isin(23, 25, 27), "Stormy")
        .when(col("coco") == 26, "Severe Storm")
        .otherwise(col("weather_condition")),
    )

    w1 = weather_df.alias("w1")
    w2 = weather_df.alias("w2")

    joined_spark_output = (
        airline_df.join(
            w1,
            (airline_df["Departure_Time_UTC"] == w1["time"])
            & (airline_df["ORIGIN"] == w1["airport"]),
            "left",
        )
        .join(
            w2,
            (airline_df["Arrival_Time_UTC"] == w2["time"])
            & (airline_df["DEST"] == w2["airport"]),
            "left",
        )
        .select(
            airline_df.FL_DATE,
            airline_df.ORIGIN,
            airline_df.DEST,
            airline_df.OP_CARRIER,
            airline_df.OP_CARRIER_FL_NUM,
            airline_df.OP_CARRIER_NAME,
            airline_df.AIRPORT_NAME_ORIGIN,
            airline_df.AIRPORT_NAME_DEST,
            airline_df.CRS_DEP_TIME,
            airline_df.DEP_TIME,
            airline_df.DEP_DELAY,
            col("Departure_Delay_Category").alias("DEP_DELAY_CATEGORY"),
            airline_df.CANCELLED,
            airline_df.DIVERTED,
            airline_df.CITY_ORIGIN,
            airline_df.STATE_ORIGIN,
            airline_df.COUNTRY_ORIGIN,
            airline_df.LONGITUDE_ORIGIN,
            airline_df.LATITUDE_ORIGIN,
            airline_df.TIME_ZONE_DEST,
            airline_df.TIME_ZONE_ORIGIN,
            col("Departure_Time_Local").alias("DEP_TIME_LOCAL"),
            col("Departure_Time_UTC").alias("DEP_TIME_UTC"),
            col("Arrival_Time_Local").alias("ARR_TIME_LOCAL"),
            col("Arrival_Time_UTC").alias("ARR_TIME_UTC"),
            airline_df.ARR_TIME,
            airline_df.ARR_DELAY,
            col("Arrival_Delay_Category").alias("ARR_DELAY_CATEGORY"),
            airline_df.CITY_DEST,
            airline_df.STATE_DEST,
            airline_df.COUNTRY_DEST,
            airline_df.LONGITUDE_DEST,
            airline_df.LATITUDE_DEST,
            col("w1.temperature_deg_c").alias("ORIGIN_TEMPERATURE_DEG_C"),
            col("w1.wind_speed_km_per_hr").alias("ORIGIN_WIND_SPEED_KM_PER_HR"),
            col("w1.relative_humidity").alias("ORIGIN_RELATIVE_HUMIDITY"),
            col("w1.precipitation").alias("ORIGIN_PRECIPITATION"),
            col("w1.weather_condition").alias("ORIGIN_WEATHER_CONDITION"),
            col("w2.temperature_deg_c").alias("DEST_TEMPERATURE_DEG_C"),
            col("w2.wind_speed_km_per_hr").alias("DEST_WIND_SPEED_KM_PER_HR"),
            col("w2.relative_humidity").alias("DEST_RELATIVE_HUMIDITY"),
            col("w2.precipitation").alias("DEST_PRECIPITATION"),
            col("w2.weather_condition").alias("DEST_WEATHER_CONDITION"),
        )
    )

    if not os.path.exists("spark_data"):
        os.makedirs("spark_data")
    print("🗒️ Saving data to CSV files...")
    # check number of rows
    print("Number of rows in depature_df: ", joined_spark_output.count())

    # Export to 1 parquet file using coalesce, into parquet
    joined_spark_output.coalesce(1).write.parquet("spark_data", mode="overwrite")
    print("✅ Data successfully saved!")


if __name__ == "__main__":
    main()

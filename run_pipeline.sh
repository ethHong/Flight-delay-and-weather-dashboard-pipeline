# Save start time
start_time=$(date +%s)
echo "⏱️ Start time: $(date)"
echo "🔗Install dependencies"

pip install kagglehub tqdm geopy pyspark findspark pandas meteostat requests bs4

#set -e

# Define Functions # Added by Jiwon
# Need to add rollback logic (delete output if Spark fails)
message() {
        printf "%50s\n" | tr " " "-"
        printf "$1\n"
        printf "%50s\n" | tr " " "-"
}

check() {
    exit_status=$?
    
    # If an exit code is provided as an argument, use it instead
    if [ $# -eq 3 ]; then
        exit_status=$3
    fi

    if [ $exit_status -eq 0 ]; then
        message "$1"
    else
        message "$2"
        rollback  # Run rollback
        exit 1  # Stop execution
    fi
}
# Rollback function

rollback() {
    echo "🔄 Rolling back... Deleting all files"
    
    # Remove Spark directory 
    rm -fr spark_data

    # Remove specific files 
    rm -f airline_delay_cancellation_data.csv airports.csv merged_weather.csv error.txt airport_ids.txt

    echo "✅ Rollback completed."
}

# Pipeline Operation 
flight_data(){
    START_YEAR=2018
    echo "🚀Starting the pipeline... Start year is $START_YEAR"
    #echo "Enter the start year: (integer only)"
    #read START_YEAR
    if [ ! -f "airline_delay_cancellation_data.csv" ] || [ ! -f "airports.csv" ]; then
        echo "⌛️run python script for flight delay data collection from $START_YEAR..."
        python get_data_and_save.py --start_year $START_YEAR
        
        exit_code=$?
        check "✅ Flight data collection completed successfully." "❌ Flight data collection failed!" $exit_code
    else
        echo "✅ Data files already exist. Moving on to the next step..."
    fi
}

weather_data(){
    if [ ! -f "merged_weather.csv" ]; then
        echo "⌛️run python script for weather data collection for flight data..."
        python get_weather.py
        
        exit_code=$?
        check "✅ Weather data collection completed successfully." "❌ Weather data collection failed!" $exit_code
    else
        echo "✅ Weather data file already exists. Moving on to the next step..."
    fi
}

run_spark(){
    if [ ! -d spark_data ]; then 
        echo "⭐️🪄Spark job for data processing..."
        spark-submit spark-job.py

        exit_code=$?
        check "✅ Spark process completed successfully." "❌ Sprak process failed!" $exit_code
        echo "Completed! 🎉 Ready to load data to Snowflake"
    else
        echo "✅ Spark data already exists. Moving on to the next step..."
    fi
}

# Snowflake Operation

run_snowflake() {
    export SNOWSQL_PWD="$SNFLK_PASSWORD"
    export SNOWSQL_ACCOUNT="$SNFLK_ACCOUNT"
    export SNOWSQL_USER="$SNFLK_USERNAME"

    ## Define filepath, rename files before loading to Snowflake # Added by Ethan
    INITIAL_JOINNED_OUTPUT=$(ls -t spark_data/part*.parquet 2>/dev/null | head -n 1)

    # Define new file names
    JOINNED_OUTPUT="spark_data/sparktbl.parquet"

    mv "$INITIAL_JOINNED_OUTPUT" "$JOINNED_OUTPUT"
    echo "✅ Renamed $INITIAL_JOINNED_OUTPUT to $JOINNED_OUTPUT"

    echo "❄️Loading the file to Snowflake Stage..."
    snowsql -q "
        USE ROLE ACCOUNTADMIN;
        USE DATABASE FINAL;
        USE SCHEMA FINAL.PUBLIC;
        USE WAREHOUSE COMPUTE_WH;
        PUT file://${JOINNED_OUTPUT} @project_stage OVERWRITE = TRUE;
    " 
    check "✅ Data loaded to Snowflake successfully" "❌ Failed to load the file to Snowflake Stage" $?

    echo "❄️Running Snowflake query"
    snowsql -f snowflake/queries.sql                                            
    check "✅ Snowflake query executed successfully." "❌ Snowflake query FAILED."
}


echo "❄️Snowflake Authentication..."
 
IFS=',' read -r SNFLK_ACCOUNT SNFLK_USERNAME SNFLK_PASSWORD < snowflakeAuth.txt

# Make snowflake credentials golbal to lead it from the funtion
export SNFLK_ACCOUNT
export SNFLK_USERNAME
export SNFLK_PASSWORD

# Run the pipeline
flight_data
weather_data
run_spark
run_snowflake

# Save end time
end_time=$(date +%s)
echo "⏱️ End time: $(date)"
echo "⏱️ Total Pipeline Elapsed Time: $((end_time - start_time)) seconds"

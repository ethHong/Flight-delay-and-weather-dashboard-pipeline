 echo "🔄 Rolling back... Deleting all files"
 # Remove Spark directory 
 rm -fr spark_data

# Remove specific files 
rm -f airline_delay_cancellation_data.csv airports.csv merged_weather.csv error.txt airport_ids.txt
 echo "✅ Rollback completed."


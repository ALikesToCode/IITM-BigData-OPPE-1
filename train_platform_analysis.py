#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, split, 
    unix_timestamp, abs as spark_abs,
    percentile_approx, expr, count, sum as spark_sum,
    isnan, isnull, desc
)
from pyspark.sql.types import IntegerType, DoubleType
import sys

def create_spark_session():
    """Create Spark session with appropriate configurations for Google Cloud"""
    return SparkSession.builder \
        .appName("TrainPlatformAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def time_to_seconds(time_col):
    """Convert HH:MM:SS time format to seconds since midnight"""
    return (
        split(time_col, ':').getItem(0).cast(IntegerType()) * 3600 +
        split(time_col, ':').getItem(1).cast(IntegerType()) * 60 +
        split(time_col, ':').getItem(2).cast(IntegerType())
    )

def calculate_stop_duration(df):
    """Calculate stop duration in minutes, handling edge cases"""
    
    # Convert arrival and departure times to seconds
    df_with_seconds = df.withColumn(
        "arrival_seconds", time_to_seconds(col("Arrival time"))
    ).withColumn(
        "departure_seconds", time_to_seconds(col("Departure Time"))
    )
    
    # Calculate stop duration in minutes
    # Handle cases where departure is next day (departure < arrival)
    df_with_duration = df_with_seconds.withColumn(
        "stop_duration_minutes",
        when(
            (col("arrival_seconds") == 0) | (col("departure_seconds") == 0),
            None  # Skip start/end stations with 00:00:00 times
        ).when(
            col("departure_seconds") >= col("arrival_seconds"),
            (col("departure_seconds") - col("arrival_seconds")) / 60.0
        ).when(
            col("departure_seconds") < col("arrival_seconds"),
            # Next day departure: add 24 hours to departure time
            ((col("departure_seconds") + 86400) - col("arrival_seconds")) / 60.0
        ).otherwise(None)
    )
    
    return df_with_duration

def get_exact_percentiles(df, percentiles):
    """Calculate exact percentiles using collect and manual calculation"""
    
    # Collect all stop durations, sorted
    durations = df.filter(col("stop_duration_minutes").isNotNull()) \
                  .select("stop_duration_minutes") \
                  .orderBy("stop_duration_minutes") \
                  .collect()
    
    duration_values = [row.stop_duration_minutes for row in durations]
    n = len(duration_values)
    
    if n == 0:
        return {}
    
    percentile_values = {}
    
    for p in percentiles:
        # Calculate exact percentile position
        pos = (p / 100.0) * (n - 1)
        
        if pos.is_integer():
            percentile_values[p] = duration_values[int(pos)]
        else:
            # Linear interpolation between two nearest values
            lower_pos = int(pos)
            upper_pos = lower_pos + 1
            
            if upper_pos < n:
                lower_val = duration_values[lower_pos]
                upper_val = duration_values[upper_pos]
                weight = pos - lower_pos
                percentile_values[p] = lower_val + weight * (upper_val - lower_val)
            else:
                percentile_values[p] = duration_values[-1]
    
    return percentile_values

def count_trains_exceeding_percentile(df, percentile_value):
    """Count number of trains that exceed the given percentile value"""
    return df.filter(
        (col("stop_duration_minutes").isNotNull()) & 
        (col("stop_duration_minutes") > percentile_value)
    ).count()

def main():
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Starting Train Platform Analysis...")
        print("=" * 50)
        
        # Read the CSV data
        print("Reading train data...")
        df = spark.read.option("header", "true").csv("data/Train_details_22122017.csv")
        
        print(f"Total records loaded: {df.count()}")
        print(f"Schema:")
        df.printSchema()
        
        # Show sample data
        print("\nSample data:")
        df.show(5, truncate=False)
        
        # Calculate stop durations
        print("\nCalculating stop durations...")
        df_with_duration = calculate_stop_duration(df)
        
        # Filter out invalid records and show statistics
        valid_durations = df_with_duration.filter(col("stop_duration_minutes").isNotNull())
        
        print(f"Records with valid stop durations: {valid_durations.count()}")
        print(f"Records filtered out (start/end stations or bad data): {df.count() - valid_durations.count()}")
        
        # Show duration statistics
        print("\nStop duration statistics:")
        valid_durations.describe("stop_duration_minutes").show()
        
        # Show sample records with durations
        print("\nSample records with calculated stop durations:")
        valid_durations.select(
            "Train No", "Station Name", "Arrival time", "Departure Time", "stop_duration_minutes"
        ).filter(col("stop_duration_minutes") > 0).show(10)
        
        # Define percentiles to calculate
        percentiles = [95, 99, 99.5, 99.95, 99.995]
        
        print(f"\nCalculating exact percentiles: {percentiles}")
        print("This may take a moment for exact calculation...")
        
        # Get exact percentile values
        percentile_values = get_exact_percentiles(valid_durations, percentiles)
        
        # Create results table
        print("\n" + "=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)
        print(f"{'Percentile':<15} {'Stop Duration (min)':<25} {'Trains Exceeding':<20}")
        print("-" * 80)
        
        results = []
        for p in percentiles:
            if p in percentile_values:
                percentile_val = percentile_values[p]
                exceeding_count = count_trains_exceeding_percentile(valid_durations, percentile_val)
                
                print(f"{p}th{'':<11} {percentile_val:<25.3f} {exceeding_count:<20}")
                results.append({
                    'percentile': p,
                    'duration': percentile_val,
                    'exceeding_count': exceeding_count
                })
        
        print("=" * 80)
        
        # Additional insights
        print("\nAdditional Insights:")
        print(f"• Total stations analyzed: {valid_durations.select('Station Name').distinct().count()}")
        print(f"• Total trains analyzed: {valid_durations.select('Train No').distinct().count()}")
        print(f"• Average stop duration: {valid_durations.agg({'stop_duration_minutes': 'avg'}).collect()[0][0]:.2f} minutes")
        print(f"• Maximum stop duration: {valid_durations.agg({'stop_duration_minutes': 'max'}).collect()[0][0]:.2f} minutes")
        print(f"• Minimum stop duration: {valid_durations.agg({'stop_duration_minutes': 'min'}).collect()[0][0]:.2f} minutes")
        
        # Show top stations by average stop duration
        print("\nTop 10 stations by average stop duration:")
        station_stats = valid_durations.groupBy("Station Name") \
            .agg(
                spark_sum("stop_duration_minutes").alias("avg_duration"),
                count("*").alias("train_count")
            ) \
            .filter(col("train_count") >= 5) \
            .orderBy(desc("avg_duration"))
        
        station_stats.show(10, truncate=False)
        
        print("\nAnalysis completed successfully!")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 
# Train Platform Analysis - Problem Statement

## Dataset Description
Train schedule data across the country including:
- Departing station
- Departure time  
- Next arriving station
- Train name
- Sequence of stations

## Problem Statement

The station master at every train station is tasked with continuously assessing whether the number of platforms available in that station at any point in time during the day is sufficient for the trains expected to stop at that station. Each station master is looking for help to solve for this challenge by analyzing how long trains typically stop and therefore use that to determine platform usage.

## Required Analysis

On the given dataset, compute the following table:

| Percentile of stop duration | Value of stop duration in minutes | Number of trains that exceed this stop duration |
|----------------------------|-----------------------------------|------------------------------------------------|
| 95th                       |                                   |                                                |
| 99th                       |                                   |                                                |
| 99.5th                     |                                   |                                                |
| 99.95th                    |                                   |                                                |
| 99.995th                   |                                   |                                                |

**Stop Duration Definition:** The difference between departure time and arrival time of a train per station, expressed in minutes.

## Implementation Suggestions

1. **First, compute the stop duration per train schedule entry**
2. **Use PySpark functions for determining appropriate percentile values per station** 
   - **DO NOT rely on approximate percentile functions**
3. **Total up number of trains that stop for a duration exceeding that particular percentile value across all stations to arrive at final answer**

## Important Notes

- **Demonstrate live outputs**
- **Do not assume the data file is sorted in ascending order by date**
- **Do not assume the data has no gaps in dates for each city & region combination**
- **Assume that there will be bad rows**

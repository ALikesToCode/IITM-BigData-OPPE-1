Train schedule across the country including departing station, departure time, next arriving station, train name, and sequence of stations.

Problem Statement: 

The station master at every train station is tasked with continuously assessing whether the number of platforms available in that station at any point in time during the day is sufficient for the trains expected to stop at that station. Each station master is looking for help to solve for this challenge by analyzing how long trains typically stop and therefore use that to determine platform usage.

 

On the given data set, compute the below table:

Percentile of stop duration

Value of stop duration in minutes

Number of trains that exceed this stop duration

95th

 	 
99th

 	 
99.5th

 	 
99.95th

 	 
99.995th

 	 
 

Here, stop duration is defined as the difference between departure time and arrival time of a train per station expressed in minutes.

Suggestions:

First, compute the stop duration per train schedule entry.
Use PySpark functions for determining appropriate percentile values per station. DO NOT rely on approximate percentile functions.
Total up number of trains that stop for a duration exceeding that particular percentile value across all stations to arrive at final answer.
Notes:

Demonstrate live outputs
Do not assume the data file is sorted in ascending order by date
Do not assume the data has no gaps in dates for each city & region combination.
Assume that there will be bad rows

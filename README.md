# Train Platform Analysis - PySpark Solution

This project analyzes train schedule data to compute platform usage statistics, calculating exact percentiles of stop duration and counting trains that exceed those thresholds.

## Problem Overview

The analysis computes stop duration (departure time - arrival time) for each train station visit and calculates:
- 95th, 99th, 99.5th, 99.95th, and 99.995th percentiles of stop duration
- Number of trains that exceed each percentile value across all stations

## Key Features

✅ **Exact Percentile Calculation** - Uses precise mathematical calculation, not approximate functions  
✅ **Robust Data Handling** - Handles bad rows, missing data, and edge cases  
✅ **Cross-day Support** - Properly handles trains that depart the next day  
✅ **Comprehensive Logging** - Detailed output showing analysis progress and results  
✅ **Google Cloud Ready** - Optimized for DataProc deployment  

## Files Description

- `train_platform_analysis.py` - Main PySpark analysis script
- `submit_to_gcloud.sh` - Automated Google Cloud submission script
- `requirements.txt` - Python dependencies
- `data/Train_details_22122017.csv` - Train schedule dataset
- `README.md` - This documentation

## Quick Start with Google Cloud

### Method 1: Automated Script (Recommended)

1. **Configure the submission script:**
   ```bash
   # Edit submit_to_gcloud.sh and update these variables:
   PROJECT_ID="your-actual-project-id"
   BUCKET_NAME="your-unique-bucket-name"
   ```

2. **Make script executable and run:**
   ```bash
   chmod +x submit_to_gcloud.sh
   ./submit_to_gcloud.sh
   ```

The script will:
- Create a DataProc cluster
- Upload your code and data to Cloud Storage
- Submit the PySpark job
- Optionally clean up the cluster after completion

### Method 2: Manual Google Cloud Steps

If you prefer manual control:

1. **Set up Google Cloud:**
   ```bash
   # Set your project
   gcloud config set project YOUR_PROJECT_ID
   
   # Create a storage bucket
   gsutil mb gs://your-bucket-name
   ```

2. **Upload files:**
   ```bash
   # Upload the Python script
   gsutil cp train_platform_analysis.py gs://your-bucket-name/
   
   # Upload the data
   gsutil cp -r data/ gs://your-bucket-name/
   ```

3. **Create DataProc cluster:**
   ```bash
   gcloud dataproc clusters create train-analysis-cluster \
       --region=us-central1 \
       --zone=us-central1-a \
       --num-workers=2 \
       --worker-machine-type=n1-standard-4 \
       --master-machine-type=n1-standard-2 \
       --image-version=2.0-debian10
   ```

4. **Submit the job:**
   ```bash
   gcloud dataproc jobs submit pyspark gs://your-bucket-name/train_platform_analysis.py \
       --cluster=train-analysis-cluster \
       --region=us-central1
   ```

5. **Clean up (optional):**
   ```bash
   gcloud dataproc clusters delete train-analysis-cluster --region=us-central1
   ```

## Local Development/Testing

To test locally (requires Spark installation):

```bash
# Install dependencies
pip install -r requirements.txt

# Run the analysis
python train_platform_analysis.py
```

## Expected Output

The analysis will produce a results table like this:

```
================================================================================
FINAL RESULTS
================================================================================
Percentile      Stop Duration (min)       Trains Exceeding    
--------------------------------------------------------------------------------
95th            XX.XXX                    XXXX                
99th            XX.XXX                    XXXX                
99.5th          XX.XXX                    XXXX                
99.95th         XX.XXX                    XXXX                
99.995th        XX.XXX                    XXXX                
================================================================================
```

Plus additional insights including:
- Total stations and trains analyzed
- Statistics (min, max, average stop duration)
- Top stations by average stop duration

## Data Processing Details

### Stop Duration Calculation
- Converts HH:MM:SS format to seconds
- Calculates difference: departure_time - arrival_time
- Handles overnight trains (departure < arrival indicates next day)
- Filters out start/end stations (00:00:00 times)

### Percentile Methodology
- Collects all valid stop durations
- Sorts data in ascending order
- Uses linear interpolation for exact percentile calculation
- No approximation functions used (as required)

### Data Quality Handling
- Filters invalid time formats
- Removes records with missing arrival/departure times
- Excludes start/end station records (typically 00:00:00)
- Reports filtering statistics for transparency

## Performance Considerations

- **Memory**: Large dataset loaded in memory for exact percentile calculation
- **Compute**: Optimized for Google Cloud DataProc with auto-scaling
- **Storage**: Uses Cloud Storage for data persistence
- **Cost**: Cluster auto-deletes after completion to minimize costs

## Troubleshooting

**Common Issues:**

1. **Permission Errors**: Ensure your Google Cloud account has DataProc and Storage permissions
2. **Bucket Errors**: Use a globally unique bucket name
3. **Memory Issues**: Increase cluster size for very large datasets
4. **Region Errors**: Ensure DataProc is available in your chosen region

**Monitoring:**
- View job progress: [Google Cloud Console](https://console.cloud.google.com/dataproc/jobs)
- Check logs: `gcloud logging read "resource.type=dataproc_cluster"`

## Cost Optimization

The script includes several cost optimization features:
- Auto-scaling cluster (scales down when not needed)
- Option to delete cluster after job completion
- Efficient data processing to minimize compute time
- Regional deployment to minimize data transfer costs

## Technical Requirements

- Google Cloud Project with billing enabled
- DataProc API enabled
- Cloud Storage API enabled
- Sufficient quota for compute instances

For questions or issues, check the Google Cloud Console logs or contact your system administrator. 
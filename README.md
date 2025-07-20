# Train Platform Analysis - PySpark Solution
# By Abhyudaya B Tharakan 22f3001492 for IBD OPPE-1
This project analyzes train schedule data to compute platform usage statistics, calculating exact percentiles of stop duration and counting trains that exceed those thresholds.

## Problem Overview

The analysis computes stop duration (departure time - arrival time) for each train station visit and calculates:
- 95th, 99th, 99.5th, 99.95th, and 99.995th percentiles of stop duration
- Number of trains that exceed each percentile value across all stations

## Key Features

✅ **Exact Percentile Calculation** - Uses precise mathematical calculation, not approximate functions  
✅ **Robust Data Handling** - Handles bad rows, missing data, and edge cases  
✅ **Advanced Data Cleaning** - Filters corrupted rows, invalid formats, and data quality issues  
✅ **Cross-day Support** - Properly handles trains that depart the next day  
✅ **Comprehensive Data Quality Report** - Shows filtering statistics and data validation  
✅ **Comprehensive Logging** - Detailed output showing analysis progress and results  
✅ **Google Cloud Ready** - Optimized for DataProc deployment  

## Files Description

- `train_platform_analysis.py` - Main PySpark analysis script with robust data validation
- `submit_to_gcloud.sh` - Automated Google Cloud deployment script with .env support
- `.env.example` - Template for environment configuration (copy to `.env`)
- `requirements.txt` - Python dependencies
- `data/Train_details_22122017.csv` - Train schedule dataset
- `README.md` - This documentation

## Quick Start with Google Cloud

### Method 1: Automated Script (Recommended)

1. **Configure your environment:**
   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit .env with your Google Cloud details
   nano .env  # or use your preferred editor
   ```
   
   **Required settings in `.env`:**
   ```bash
   PROJECT_ID=your-actual-project-id
   BUCKET_NAME=your-globally-unique-bucket-name
   ```
   
   **Optional settings** (defaults will be used if not specified):
   ```bash
   CLUSTER_NAME=train-analysis-cluster
   REGION=us-central1
   NUM_WORKERS=2
   WORKER_MACHINE_TYPE=n1-standard-4
   MAX_WORKERS=5
   ```

2. **Run the deployment:**
   ```bash
   ./submit_to_gcloud.sh
   ```

   **🔒 Security Note:** The `.env` file contains sensitive information and is automatically excluded from version control via `.gitignore`.

### Method 2: Manual Google Cloud Steps

If you prefer manual control:

1. **Set up Google Cloud:**
   ```bash
   # Set your project
   gcloud config set project YOUR_PROJECT_ID
   
   # Create a storage bucket
   gcloud storage buckets create gs://your-bucket-name --location=us-central1
   ```

2. **Upload files:**
   ```bash
   # Upload the Python script
   gcloud storage cp train_platform_analysis.py gs://your-bucket-name/
   
   # Upload the data
   gcloud storage cp -r data/ gs://your-bucket-name/
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

The analysis will produce comprehensive results including:

### Data Quality Report
```
================================================================================
📋 DATA QUALITY REPORT
================================================================================
📥 Original records:           186,124
🧹 After data cleaning:       186,119 (100.0%)
✅ Valid stop durations:      182,147 (97.9%)
❌ Records filtered out:      3,977 (2.1%)
================================================================================
```

### Final Results
```
================================================================================
🎉 FINAL RESULTS - ROBUST ANALYSIS
================================================================================
Percentile      Stop Duration (min)       Trains Exceeding    
--------------------------------------------------------------------------------
95th            10.000                    4463                
99th            20.000                    1535                
99.5th          25.000                    816                 
99.95th         55.000                    86                  
99.995th        154.463                   10                  
================================================================================
```

Plus additional insights including:
- Total stations and trains analyzed
- Statistics (min, max, average stop duration)
- Top stations by total stop duration
- Comprehensive data quality breakdown

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
- **Comprehensive Data Validation**: Validates train numbers, time formats, and field integrity
- **Corruption Detection**: Identifies and filters rows with data corruption (station codes in time fields)
- **Smart Filtering**: Removes duplicate headers, invalid formats, and unrealistic values
- **Robust Time Parsing**: Handles malformed times, NA values, and encoding issues
- **Quality Reporting**: Provides detailed statistics on data cleaning and filtering
- **Station Name Cleaning**: Removes special characters and normalizes station names

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
- **Modern gcloud CLI** (v380.0.0+) with `gcloud storage` commands
- Sufficient quota for compute instances

## Modern gcloud Commands

This solution uses the modern `gcloud storage` commands instead of the legacy `gsutil` commands:

**✅ Modern (Used):**
```bash
gcloud storage buckets create gs://bucket-name --location=us-central1
gcloud storage cp file.py gs://bucket-name/
gcloud storage cp -r data/ gs://bucket-name/
```

**❌ Legacy (Deprecated):**
```bash
gsutil mb gs://bucket-name
gsutil cp file.py gs://bucket-name/
gsutil cp -r data/ gs://bucket-name/
```

**Benefits of `gcloud storage`:**
- Better performance and reliability
- Improved error handling and retry logic
- Consistent CLI experience across Google Cloud services
- Enhanced progress indicators
- Better integration with IAM and security features

For questions or issues, check the Google Cloud Console logs or contact your system administrator. 
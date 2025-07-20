#!/bin/bash

# Configuration - Update these variables with your Google Cloud details
PROJECT_ID="your-project-id"
CLUSTER_NAME="train-analysis-cluster"
REGION="us-central1"
ZONE="us-central1-a"
BUCKET_NAME="your-bucket-name"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Train Platform Analysis on Google Cloud${NC}"
echo "============================================="

# Check if required variables are set
if [[ "$PROJECT_ID" == "your-project-id" || "$BUCKET_NAME" == "your-bucket-name" ]]; then
    echo -e "${RED}Error: Please update PROJECT_ID and BUCKET_NAME in this script${NC}"
    echo "Set your actual Google Cloud project ID and bucket name"
    exit 1
fi

# Set the project
echo -e "${YELLOW}Setting Google Cloud project...${NC}"
gcloud config set project $PROJECT_ID

# Create bucket if it doesn't exist
echo -e "${YELLOW}Creating/verifying storage bucket...${NC}"
gsutil mb gs://$BUCKET_NAME 2>/dev/null || echo "Bucket already exists"

# Upload data and script to Google Cloud Storage
echo -e "${YELLOW}Uploading files to Google Cloud Storage...${NC}"
gsutil cp train_platform_analysis.py gs://$BUCKET_NAME/
gsutil cp -r data/ gs://$BUCKET_NAME/

# Check if cluster exists, create if not
echo -e "${YELLOW}Checking DataProc cluster...${NC}"
if ! gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION &> /dev/null; then
    echo -e "${YELLOW}Creating DataProc cluster...${NC}"
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION \
        --zone=$ZONE \
        --num-workers=2 \
        --worker-machine-type=n1-standard-4 \
        --worker-boot-disk-size=50GB \
        --master-machine-type=n1-standard-2 \
        --master-boot-disk-size=50GB \
        --image-version=2.0-debian10 \
        --enable-autoscaling \
        --max-workers=5 \
        --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
        --metadata="PIP_PACKAGES=pyspark>=3.3.0" \
        --enable-ip-alias
        
    echo -e "${GREEN}Cluster created successfully${NC}"
else
    echo -e "${GREEN}Cluster already exists${NC}"
fi

# Submit the PySpark job
echo -e "${YELLOW}Submitting PySpark job...${NC}"
gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/train_platform_analysis.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

echo -e "${GREEN}Job submitted successfully!${NC}"
echo -e "${YELLOW}You can monitor the job progress in the Google Cloud Console:${NC}"
echo "https://console.cloud.google.com/dataproc/jobs?project=$PROJECT_ID&region=$REGION"

# Optionally delete cluster after job completion
read -p "Do you want to delete the cluster after the job completes? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Cluster will be deleted after job completion...${NC}"
    gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
    echo -e "${GREEN}Cluster deleted${NC}"
fi

echo -e "${GREEN}Process completed!${NC}" 
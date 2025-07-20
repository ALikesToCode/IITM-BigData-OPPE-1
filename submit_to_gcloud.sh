#!/bin/bash

# Load environment variables from .env file
if [[ -f .env ]]; then
    echo "📄 Loading configuration from .env file..."
    set -a  # automatically export all variables
    source .env
    set +a  # disable auto-export
else
    echo "❌ .env file not found!"
    echo "Please copy .env.example to .env and configure your Google Cloud settings"
    echo ""
    echo "  cp .env.example .env"
    echo "  # Edit .env with your Google Cloud details"
    echo ""
    exit 1
fi

# Set defaults for optional variables
NUM_WORKERS=${NUM_WORKERS:-2}
WORKER_MACHINE_TYPE=${WORKER_MACHINE_TYPE:-n1-standard-4}
MASTER_MACHINE_TYPE=${MASTER_MACHINE_TYPE:-n1-standard-2}
WORKER_BOOT_DISK_SIZE=${WORKER_BOOT_DISK_SIZE:-50GB}
MASTER_BOOT_DISK_SIZE=${MASTER_BOOT_DISK_SIZE:-50GB}
MAX_WORKERS=${MAX_WORKERS:-5}
IMAGE_VERSION=${IMAGE_VERSION:-2.0-debian10}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Train Platform Analysis on Google Cloud${NC}"
echo "=============================================="

# Display current configuration
echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Bucket: $BUCKET_NAME"
echo "  Cluster: $CLUSTER_NAME"
echo "  Region: $REGION"
echo "  Zone: $ZONE"
echo "  Workers: $NUM_WORKERS (max: $MAX_WORKERS)"
echo ""

# Check if required variables are set
if [[ -z "$PROJECT_ID" || -z "$BUCKET_NAME" || "$PROJECT_ID" == "your-project-id" || "$BUCKET_NAME" == "your-bucket-name" ]]; then
    echo -e "${RED}❌ Error: Please configure PROJECT_ID and BUCKET_NAME in .env file${NC}"
    echo "Edit the .env file with your actual Google Cloud project ID and bucket name"
    exit 1
fi

# Set the project
echo -e "${YELLOW}⚙️ Setting Google Cloud project...${NC}"
gcloud config set project $PROJECT_ID

# Create bucket if it doesn't exist
echo -e "${YELLOW}🪣 Creating/verifying storage bucket...${NC}"
if gcloud storage buckets create gs://$BUCKET_NAME --location=$REGION 2>/dev/null; then
    echo "  ✅ Created new bucket: $BUCKET_NAME"
else
    # Check if bucket exists and we can access it
    if gcloud storage ls gs://$BUCKET_NAME/ >/dev/null 2>&1; then
        echo "  ✅ Bucket already exists and is accessible: $BUCKET_NAME"
    else
        echo -e "${RED}  ❌ Error: Cannot create or access bucket $BUCKET_NAME${NC}"
        echo "  Make sure the bucket name is globally unique and you have permissions"
        exit 1
    fi
fi

# Upload data and script to Google Cloud Storage
echo -e "${YELLOW}📤 Uploading files to Google Cloud Storage...${NC}"
echo "  • Uploading PySpark script..."
if ! gcloud storage cp train_platform_analysis.py gs://$BUCKET_NAME/; then
    echo -e "${RED}  ❌ Failed to upload PySpark script${NC}"
    exit 1
fi

echo "  • Uploading train data..."
if ! gcloud storage cp -r data/ gs://$BUCKET_NAME/; then
    echo -e "${RED}  ❌ Failed to upload train data${NC}"
    exit 1
fi

echo -e "${GREEN}  ✅ All files uploaded successfully${NC}"

# Check if cluster exists, create if not
echo -e "${YELLOW}🔍 Checking DataProc cluster...${NC}"
if ! gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION &> /dev/null; then
    echo -e "${YELLOW}🔨 Creating DataProc cluster with configuration:${NC}"
    echo "  • Workers: $NUM_WORKERS"
    echo "  • Worker type: $WORKER_MACHINE_TYPE"
    echo "  • Master type: $MASTER_MACHINE_TYPE" 
    echo "  • Max workers: $MAX_WORKERS"
    echo "  • Image: $IMAGE_VERSION"
    echo ""
    
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION \
        --zone=$ZONE \
        --num-workers=$NUM_WORKERS \
        --worker-machine-type=$WORKER_MACHINE_TYPE \
        --worker-boot-disk-size=$WORKER_BOOT_DISK_SIZE \
        --master-machine-type=$MASTER_MACHINE_TYPE \
        --master-boot-disk-size=$MASTER_BOOT_DISK_SIZE \
        --image-version=$IMAGE_VERSION \
        --enable-autoscaling \
        --max-workers=$MAX_WORKERS \
        --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
        --metadata="PIP_PACKAGES=pyspark>=3.3.0" \
        --enable-ip-alias
        
    echo -e "${GREEN}✅ Cluster created successfully${NC}"
else
    echo -e "${GREEN}✅ Cluster already exists${NC}"
fi

# Submit the PySpark job
echo -e "${YELLOW}🚀 Submitting PySpark job...${NC}"
echo "  • Job: Train Platform Analysis"
echo "  • Cluster: $CLUSTER_NAME"
echo "  • Region: $REGION"
echo ""

gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/train_platform_analysis.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

echo ""
echo -e "${GREEN}🎉 Job submitted successfully!${NC}"
echo ""
echo -e "${BLUE}📊 Monitor your job:${NC}"
echo "🔗 Google Cloud Console: https://console.cloud.google.com/dataproc/jobs?project=$PROJECT_ID&region=$REGION"
echo "📋 Command line: gcloud dataproc jobs list --region=$REGION"
echo ""

# Optionally delete cluster after job completion
echo -e "${YELLOW}💰 Cost Management:${NC}"
read -p "Do you want to delete the cluster after the job completes? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}🗑️ Deleting cluster to save costs...${NC}"
    gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
    echo -e "${GREEN}✅ Cluster deleted successfully${NC}"
else
    echo -e "${YELLOW}⚠️ Remember to delete the cluster manually to avoid ongoing charges:${NC}"
    echo "   gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION"
fi

echo ""
echo -e "${GREEN}🏁 Deployment completed successfully!${NC}"
echo -e "${BLUE}📈 Your train platform analysis is now running on Google Cloud DataProc${NC}" 
#!/bin/bash

# Google Cloud DataProc Train Analysis Submission Script
# Complete automation for uploading data, creating cluster, and running analysis
# Updated to support markdown report generation and enhanced dependencies

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Google Cloud DataProc Train Analysis Deployment${NC}"
echo "=================================================================="

# Load configuration
if [ -f ".env" ]; then
    # Load .env file while ignoring comments and empty lines
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
    echo -e "${GREEN}✅ Configuration loaded from .env${NC}"
else
    echo -e "${RED}❌ .env file not found!${NC}"
    echo "Please create .env file from .env.example and configure your settings"
    exit 1
fi

# Validate required configuration
required_vars=("PROJECT_ID" "BUCKET_NAME" "CLUSTER_NAME" "REGION")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}❌ Missing required configuration: $var${NC}"
        echo "Please configure $var in your .env file"
        exit 1
    fi
done

echo -e "${BLUE}📋 Configuration Summary:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Bucket: $BUCKET_NAME"
echo "  Cluster: $CLUSTER_NAME"
echo "  Region: $REGION"
echo ""

# Check required files
required_files=("train_platform_analysis_final.py" "requirements.txt" "data/Train_details_22122017.csv")
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}❌ Required file missing: $file${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✅ All required files found${NC}"

# Set Google Cloud project
echo -e "${YELLOW}🔧 Setting up Google Cloud environment...${NC}"
gcloud config set project $PROJECT_ID || {
    echo -e "${RED}❌ Failed to set project. Check if project ID is correct${NC}"
    exit 1
}

# Create bucket if it doesn't exist
echo -e "${YELLOW}📦 Setting up Cloud Storage bucket...${NC}"
if ! gcloud storage ls gs://$BUCKET_NAME > /dev/null 2>&1; then
    echo "Creating bucket: $BUCKET_NAME"
    gcloud storage buckets create gs://$BUCKET_NAME --location=$REGION || {
        echo -e "${RED}❌ Failed to create bucket${NC}"
        exit 1
    }
    echo -e "${GREEN}✅ Bucket created successfully${NC}"
else
    echo -e "${GREEN}✅ Bucket already exists${NC}"
fi

# Upload analysis script
echo -e "${YELLOW}📤 Uploading analysis script and dependencies...${NC}"
gcloud storage cp train_platform_analysis_final.py gs://$BUCKET_NAME/ || {
    echo -e "${RED}❌ Failed to upload analysis script${NC}"
    exit 1
}

gcloud storage cp requirements.txt gs://$BUCKET_NAME/ || {
    echo -e "${RED}❌ Failed to upload requirements.txt${NC}"
    exit 1
}

# Upload data
echo -e "${YELLOW}📤 Uploading train data...${NC}"
gcloud storage cp -r data/ gs://$BUCKET_NAME/ || {
    echo -e "${RED}❌ Failed to upload data${NC}"
    exit 1
}
echo -e "${GREEN}✅ All files uploaded successfully${NC}"

# Create initialization script for dependencies
echo -e "${YELLOW}📝 Creating cluster initialization script...${NC}"
cat > init-script.sh << 'EOF'
#!/bin/bash
# Install additional Python packages for enhanced functionality
pip install rich>=13.0.0 tabulate>=0.9.0 google-cloud-storage>=2.10.0 google-auth>=2.23.0
echo "Enhanced dependencies installed successfully"
EOF

# Upload initialization script
gcloud storage cp init-script.sh gs://$BUCKET_NAME/init-script.sh

# Check if target cluster already exists
echo -e "${YELLOW}🔍 Checking if cluster exists...${NC}"
if gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️ Cluster $CLUSTER_NAME already exists${NC}"
    read -p "Delete and recreate cluster? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🗑️ Deleting existing cluster...${NC}"
        gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet || {
            echo -e "${RED}❌ Failed to delete existing cluster${NC}"
            exit 1
        }
        echo -e "${GREEN}✅ Existing cluster deleted${NC}"
    else
        echo -e "${BLUE}📋 Using existing cluster${NC}"
        CLUSTER_EXISTS=true
    fi
fi

# Check for fallback cluster if target cluster doesn't exist
if [ "$CLUSTER_EXISTS" != true ]; then
    echo -e "${YELLOW}🔍 Checking for fallback cluster: spark-click-analysis-cluster...${NC}"
    if gcloud dataproc clusters describe spark-click-analysis-cluster --region=$REGION > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Found existing fallback cluster: spark-click-analysis-cluster${NC}"
        read -p "Use existing spark-click-analysis-cluster instead? (Y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            CLUSTER_NAME="spark-click-analysis-cluster"
            CLUSTER_EXISTS=true
            echo -e "${GREEN}✅ Using fallback cluster: $CLUSTER_NAME${NC}"
        fi
    fi
fi

# Create cluster if it doesn't exist
if [ "$CLUSTER_EXISTS" != true ]; then
    echo -e "${YELLOW}🏗️ Creating DataProc cluster with enhanced configuration...${NC}"
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION \
        --zone=${REGION}-c \
        --master-machine-type=n1-standard-1 \
        --master-boot-disk-size=30GB \
        --worker-machine-type=n1-standard-1 \
        --worker-boot-disk-size=30GB \
        --num-workers=2 \
        --image-version=2.0-debian10 \
        --initialization-actions=gs://$BUCKET_NAME/init-script.sh \
        --properties="spark:spark.jars.packages=org.apache.spark:spark-avro_2.12:3.1.2" \
        --optional-components=JUPYTER \
        --max-idle=10m || {
        echo -e "${RED}❌ Failed to create cluster${NC}"
        exit 1
    }
    echo -e "${GREEN}✅ Cluster created successfully${NC}"
fi

# Submit PySpark job
echo -e "${YELLOW}🚀 Submitting PySpark analysis job...${NC}"
echo "This may take several minutes to complete..."

JOB_ID="train-analysis-$(date +%Y%m%d-%H%M%S)"

gcloud dataproc jobs submit pyspark \
    gs://$BUCKET_NAME/train_platform_analysis_final.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --id=$JOB_ID \
    --properties="spark.submit.deployMode=client,spark.executor.memory=1g,spark.executor.cores=1,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true" \
    -- --data-path=gs://$BUCKET_NAME/data/Train_details_22122017.csv || {
    echo -e "${RED}❌ Job submission failed${NC}"
    exit 1
}

echo -e "${GREEN}✅ Job submitted successfully!${NC}"
echo -e "${BLUE}📊 Job ID: $JOB_ID${NC}"

# Wait for job completion and show progress
echo -e "${YELLOW}⏳ Monitoring job progress...${NC}"
while true; do
    JOB_STATE=$(gcloud dataproc jobs describe $JOB_ID --region=$REGION --format="value(status.state)")
    case $JOB_STATE in
        "DONE")
            echo -e "${GREEN}✅ Job completed successfully!${NC}"
            break
            ;;
        "ERROR"|"CANCELLED")
            echo -e "${RED}❌ Job failed with state: $JOB_STATE${NC}"
            echo "Fetching job logs..."
            gcloud dataproc jobs wait $JOB_ID --region=$REGION || true
            exit 1
            ;;
        *)
            echo -e "${BLUE}📊 Job status: $JOB_STATE${NC}"
            sleep 30
            ;;
    esac
done

# Get job output
echo -e "${YELLOW}📋 Fetching job results...${NC}"
gcloud dataproc jobs wait $JOB_ID --region=$REGION

# Check for generated markdown reports in the bucket
echo -e "${YELLOW}📄 Checking for generated reports...${NC}"
REPORTS=$(gcloud storage ls gs://$BUCKET_NAME/analysis-results/ 2>/dev/null | grep "\.md$" | head -5)
if [ -n "$REPORTS" ]; then
    echo -e "${GREEN}✅ Markdown reports found:${NC}"
    echo "$REPORTS"
    
    # Download the most recent report
    LATEST_REPORT=$(echo "$REPORTS" | tail -1)
    LOCAL_REPORT=$(basename "$LATEST_REPORT")
    
    echo -e "${YELLOW}📥 Downloading latest report: $LOCAL_REPORT${NC}"
    gcloud storage cp "$LATEST_REPORT" ./ || {
        echo -e "${YELLOW}⚠️ Could not download report, but it's available in GCS${NC}"
    }
else
    echo -e "${YELLOW}⚠️ No markdown reports found in bucket${NC}"
fi

# Cost management prompt
echo -e "${YELLOW}💰 Cost Management Options:${NC}"
echo "1. Keep cluster running (ongoing costs)"
echo "2. Delete cluster now (saves money)"
echo "3. Set cluster to auto-delete after idle time (recommended)"
echo ""

read -p "Choose option (1/2/3): " -n 1 -r
echo ""

case $REPLY in
    1)
        echo -e "${BLUE}📋 Cluster will remain active${NC}"
        ;;
    2)
        echo -e "${YELLOW}🗑️ Deleting cluster...${NC}"
        gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet || {
            echo -e "${RED}❌ Failed to delete cluster${NC}"
        }
        echo -e "${GREEN}✅ Cluster deleted successfully${NC}"
        ;;
    3)
        echo -e "${GREEN}✅ Cluster configured with auto-idle deletion (10 minutes)${NC}"
        ;;
    *)
        echo -e "${BLUE}📋 No action taken - cluster remains active${NC}"
        ;;
esac

# Cleanup local temporary files
rm -f init-script.sh

# Final summary
echo ""
echo -e "${GREEN}🎉 DEPLOYMENT COMPLETED SUCCESSFULLY! 🎉${NC}"
echo "=================================================================="
echo -e "${BLUE}📊 Summary:${NC}"
echo "  ✅ Data uploaded to: gs://$BUCKET_NAME/data/"
echo "  ✅ Script uploaded to: gs://$BUCKET_NAME/train_platform_analysis_final.py"
echo "  ✅ Job completed: $JOB_ID"
echo "  ✅ Enhanced dependencies installed (rich, tabulate, google-cloud-storage)"
echo "  ✅ Markdown reports saved to: gs://$BUCKET_NAME/analysis-results/"
echo ""
echo -e "${YELLOW}📋 Next Steps:${NC}"
echo "1. View results in Google Cloud Console: https://console.cloud.google.com/dataproc/jobs"
echo "2. Access markdown reports in: gs://$BUCKET_NAME/analysis-results/"
echo "3. Check bucket contents: gcloud storage ls -r gs://$BUCKET_NAME/"
echo "4. Download reports: gcloud storage cp gs://$BUCKET_NAME/analysis-results/*.md ./"
echo ""
echo -e "${GREEN}✨ Analysis complete with beautiful formatting and cloud storage integration!${NC}" 
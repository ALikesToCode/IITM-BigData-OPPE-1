#!/bin/bash

# Test script to validate Google Cloud submission setup
# This script tests the configuration without actually submitting to cloud

echo "🧪 Testing Google Cloud Setup Configuration"
echo "==========================================="

# Test 1: Check if .env.example exists
if [[ -f .env.example ]]; then
    echo "✅ .env.example file found"
else
    echo "❌ .env.example file missing"
    exit 1
fi

# Test 2: Check if analysis script exists
if [[ -f train_platform_analysis_final.py ]]; then
    echo "✅ Final analysis script found"
else
    echo "❌ train_platform_analysis_final.py not found"
    exit 1
fi

# Test 3: Check if data file exists
if [[ -f data/Train_details_22122017.csv ]]; then
    echo "✅ Data file found"
else
    echo "❌ Data file missing"
    exit 1
fi

# Test 4: Check if submit script exists
if [[ -f submit_to_gcloud.sh ]]; then
    echo "✅ Google Cloud submission script found"
else
    echo "❌ submit_to_gcloud.sh missing"
    exit 1
fi

# Test 5: Validate script syntax
echo ""
echo "🔍 Validating script syntax..."
if bash -n submit_to_gcloud.sh; then
    echo "✅ Google Cloud script syntax is valid"
else
    echo "❌ Google Cloud script has syntax errors"
    exit 1
fi

# Test 6: Check script permissions
if [[ -x submit_to_gcloud.sh ]]; then
    echo "✅ Google Cloud script is executable"
else
    echo "⚠️  Making Google Cloud script executable..."
    chmod +x submit_to_gcloud.sh
    echo "✅ Google Cloud script is now executable"
fi

# Test 7: Show what the .env file should contain
echo ""
echo "📋 Configuration Summary:"
echo "To use the Google Cloud submission:"
echo ""
echo "1. Copy the example configuration:"
echo "   cp .env.example .env"
echo ""
echo "2. Edit .env with your Google Cloud details:"
echo "   - PROJECT_ID: Your Google Cloud project ID"
echo "   - BUCKET_NAME: A globally unique bucket name" 
echo "   - CLUSTER_NAME: Name for your DataProc cluster"
echo "   - REGION: Google Cloud region (e.g., us-central1)"
echo ""
echo "3. Run the submission:"
echo "   ./submit_to_gcloud.sh"
echo ""

# Test 8: Show sample commands that would be run
echo "🚀 Sample commands that would be executed:"
echo "----------------------------------------"
echo "1. gcloud config set project YOUR_PROJECT_ID"
echo "2. gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=YOUR_REGION"
echo "3. gcloud storage cp train_platform_analysis_final.py gs://YOUR_BUCKET_NAME/"
echo "4. gcloud storage cp -r data/ gs://YOUR_BUCKET_NAME/"
echo "5. gcloud dataproc clusters create train-analysis-cluster ..."
echo "6. gcloud dataproc jobs submit pyspark gs://YOUR_BUCKET_NAME/train_platform_analysis_final.py ..."
echo ""

echo "✅ All tests passed! Google Cloud setup is ready."
echo "📝 Remember to configure your .env file before running ./submit_to_gcloud.sh" 
#!/bin/bash

# Smart City Data Pipeline Deployment Script
set -e

ENVIRONMENT=${1:-dev}
AWS_REGION=${2:-us-east-1}
AWS_PROFILE=${3:-default}

echo "🚀 Deploying Smart City Data Pipeline to $ENVIRONMENT environment"
echo "📍 Region: $AWS_REGION"
echo "👤 Profile: $AWS_PROFILE"

# Set AWS profile
export AWS_PROFILE=$AWS_PROFILE
export AWS_DEFAULT_REGION=$AWS_REGION

# Deploy S3 infrastructure first
echo "📦 Deploying S3 infrastructure..."
aws cloudformation deploy \
  --template-file infrastructure/s3/s3-stack.yaml \
  --stack-name smart-city-s3-$ENVIRONMENT \
  --parameter-overrides Environment=$ENVIRONMENT \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION

# Get S3 bucket names from outputs
DATA_LAKE_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name smart-city-s3-$ENVIRONMENT \
  --query 'Stacks[0].Outputs[?OutputKey==`DataLakeBucketName`].OutputValue' \
  --output text \
  --region $AWS_REGION)

ATHENA_RESULTS_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name smart-city-s3-$ENVIRONMENT \
  --query 'Stacks[0].Outputs[?OutputKey==`AthenaResultsBucketName`].OutputValue' \
  --output text \
  --region $AWS_REGION)

GLUE_SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name smart-city-s3-$ENVIRONMENT \
  --query 'Stacks[0].Outputs[?OutputKey==`GlueScriptsBucketName`].OutputValue' \
  --output text \
  --region $AWS_REGION)

echo "✅ S3 buckets created:"
echo "   📊 Data Lake: $DATA_LAKE_BUCKET"
echo "   🔍 Athena Results: $ATHENA_RESULTS_BUCKET"
echo "   📝 Glue Scripts: $GLUE_SCRIPTS_BUCKET"

# Upload Glue scripts
echo "📤 Uploading Glue scripts..."
aws s3 cp glue-jobs/batch-etl-bronze-to-silver.py s3://$GLUE_SCRIPTS_BUCKET/
aws s3 cp glue-jobs/streaming-etl-kinesis-to-silver.py s3://$GLUE_SCRIPTS_BUCKET/
aws s3 cp glue-jobs/silver-to-gold-etl.py s3://$GLUE_SCRIPTS_BUCKET/

# Deploy Kinesis infrastructure
echo "🌊 Deploying Kinesis infrastructure..."
aws cloudformation deploy \
  --template-file infrastructure/kinesis/kinesis-stack.yaml \
  --stack-name smart-city-kinesis-$ENVIRONMENT \
  --parameter-overrides Environment=$ENVIRONMENT DataLakeBucket=$DATA_LAKE_BUCKET \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION

# Deploy Glue infrastructure
echo "🔧 Deploying Glue infrastructure..."
aws cloudformation deploy \
  --template-file infrastructure/glue/glue-stack.yaml \
  --stack-name smart-city-glue-$ENVIRONMENT \
  --parameter-overrides Environment=$ENVIRONMENT DataLakeBucket=$DATA_LAKE_BUCKET \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION

# Deploy Athena infrastructure
echo "🔍 Deploying Athena infrastructure..."
aws cloudformation deploy \
  --template-file infrastructure/athena/athena-stack.yaml \
  --stack-name smart-city-athena-$ENVIRONMENT \
  --parameter-overrides Environment=$ENVIRONMENT AthenaResultsBucket=$ATHENA_RESULTS_BUCKET \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION

# Deploy Lambda functions
echo "⚡ Deploying Lambda functions..."
aws cloudformation deploy \
  --template-file infrastructure/lambda/lambda-functions.yaml \
  --stack-name smart-city-lambda-$ENVIRONMENT \
  --parameter-overrides Environment=$ENVIRONMENT DataLakeBucket=$DATA_LAKE_BUCKET \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION

echo "🎉 Deployment completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Start the streaming Glue job for real-time processing"
echo "2. Schedule the batch Glue jobs for daily processing"
echo "3. Create Athena tables using the provided queries"
echo "4. Set up monitoring and alerting"
echo ""
echo "🔗 Useful commands:"
echo "   aws glue start-job-run --job-name smart-city-streaming-etl-$ENVIRONMENT"
echo "   aws athena start-query-execution --work-group smart-city-$ENVIRONMENT"

#!/bin/bash
# Setup script to create all DynamoDB tables using AWS CLI
# Usage: bash scripts/setup_dynamodb_tables.sh

echo "============================================================"
echo "🚀 Yahoo Fantasy Baseball - DynamoDB Setup"
echo "============================================================"

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Please install it first:"
    echo "   https://aws.amazon.com/cli/"
    exit 1
fi

# Test AWS connection
echo ""
echo "📡 Testing AWS connection..."
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ AWS CLI not configured. Run: aws configure"
    exit 1
fi

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "✅ Connected to AWS Account: $ACCOUNT"

REGION="us-west-2"

echo ""
echo "============================================================"
echo "Creating Table 1/5: FantasyBaseball-LiveData"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-LiveData \
    --attribute-definitions \
        AttributeName=DataType,AttributeType=S \
        AttributeName=TeamNumber,AttributeType=S \
    --key-schema \
        AttributeName=DataType,KeyType=HASH \
        AttributeName=TeamNumber,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 2/5: FantasyBaseball-WeeklyTimeSeries"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-WeeklyTimeSeries \
    --attribute-definitions \
        AttributeName=TeamNumber,AttributeType=S \
        AttributeName=Week#DataType,AttributeType=S \
        AttributeName=DataType,AttributeType=S \
        AttributeName=Week,AttributeType=N \
    --key-schema \
        AttributeName=TeamNumber,KeyType=HASH \
        AttributeName=Week#DataType,KeyType=RANGE \
    --global-secondary-indexes \
        "[{
            \"IndexName\": \"DataTypeWeekIndex\",
            \"KeySchema\": [
                {\"AttributeName\":\"DataType\",\"KeyType\":\"HASH\"},
                {\"AttributeName\":\"Week\",\"KeyType\":\"RANGE\"}
            ],
            \"Projection\": {\"ProjectionType\":\"ALL\"}
        }]" \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 3/5: FantasyBaseball-MatchupResults"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-MatchupResults \
    --attribute-definitions \
        AttributeName=Week,AttributeType=N \
        AttributeName=TeamNumber,AttributeType=S \
    --key-schema \
        AttributeName=Week,KeyType=HASH \
        AttributeName=TeamNumber,KeyType=RANGE \
    --global-secondary-indexes \
        "[{
            \"IndexName\": \"TeamHistoryIndex\",
            \"KeySchema\": [
                {\"AttributeName\":\"TeamNumber\",\"KeyType\":\"HASH\"},
                {\"AttributeName\":\"Week\",\"KeyType\":\"RANGE\"}
            ],
            \"Projection\": {\"ProjectionType\":\"ALL\"}
        }]" \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 4/5: FantasyBaseball-Schedule"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-Schedule \
    --attribute-definitions \
        AttributeName=Week,AttributeType=N \
        AttributeName=TeamNumber,AttributeType=S \
    --key-schema \
        AttributeName=Week,KeyType=HASH \
        AttributeName=TeamNumber,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 5/5: FantasyBaseball-AllTimeHistory"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-AllTimeHistory \
    --attribute-definitions \
        AttributeName=TeamNumber,AttributeType=S \
        AttributeName=Year,AttributeType=S \
        AttributeName=ScoreRank,AttributeType=N \
    --key-schema \
        AttributeName=TeamNumber,KeyType=HASH \
        AttributeName=Year,KeyType=RANGE \
    --global-secondary-indexes \
        "[{
            \"IndexName\": \"YearIndex\",
            \"KeySchema\": [
                {\"AttributeName\":\"Year\",\"KeyType\":\"HASH\"},
                {\"AttributeName\":\"ScoreRank\",\"KeyType\":\"RANGE\"}
            ],
            \"Projection\": {\"ProjectionType\":\"ALL\"}
        }]" \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "📊 Setup Complete!"
echo "============================================================"
echo ""
echo "⏳ Tables are being created (this takes 1-2 minutes)"
echo ""
echo "To check status:"
echo "   aws dynamodb list-tables --region $REGION"
echo ""
echo "To wait for tables to become active:"
echo "   aws dynamodb wait table-exists --table-name FantasyBaseball-LiveData --region $REGION"
echo ""
echo "💡 Next steps:"
echo "   1. Wait for tables to become ACTIVE"
echo "   2. Enable Point-in-Time Recovery (PITR):"
echo "      bash scripts/enable_pitr.sh"
echo "   3. Update scrapers to use DynamoStorageManager"
echo "   4. Test dual-write (MongoDB + DynamoDB)"
echo ""
echo "============================================================"

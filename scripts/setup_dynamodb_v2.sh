#!/bin/bash
# Setup DynamoDB tables - 3 Table Design (Optimized)
# Usage: bash scripts/setup_dynamodb_v2.sh

echo "============================================================"
echo "🚀 Yahoo Fantasy Baseball - DynamoDB Setup (v2)"
echo "============================================================"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found"
    exit 1
fi

# Test AWS connection
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "❌ AWS CLI not configured. Run: aws configure"
    exit 1
fi

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "✅ Connected to AWS Account: $ACCOUNT"

REGION="us-west-2"

echo ""
echo "============================================================"
echo "Creating Table 1/3: FantasyBaseball-SeasonTrends"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-SeasonTrends \
    --attribute-definitions \
        AttributeName=TeamNumber,AttributeType=S \
        AttributeName=DataType#Week,AttributeType=S \
        AttributeName=DataTypeWeek,AttributeType=S \
    --key-schema \
        AttributeName=TeamNumber,KeyType=HASH \
        AttributeName=DataType#Week,KeyType=RANGE \
    --global-secondary-indexes \
        "[{
            \"IndexName\": \"DataTypeWeekIndex\",
            \"KeySchema\": [
                {\"AttributeName\":\"DataTypeWeek\",\"KeyType\":\"HASH\"}
            ],
            \"Projection\": {\"ProjectionType\":\"ALL\"}
        }]" \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 2/3: FantasyBaseball-TeamInfo"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-TeamInfo \
    --attribute-definitions \
        AttributeName=TeamNumber,AttributeType=S \
    --key-schema \
        AttributeName=TeamNumber,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "Creating Table 3/3: FantasyBaseball-Schedule"
echo "============================================================"

aws dynamodb create-table \
    --table-name FantasyBaseball-Schedule \
    --attribute-definitions \
        AttributeName=Week,AttributeType=N \
        AttributeName=MatchupID,AttributeType=S \
    --key-schema \
        AttributeName=Week,KeyType=HASH \
        AttributeName=MatchupID,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value=YahooFantasyBaseball Key=Environment,Value=Production \
    --region $REGION \
    2>&1 | grep -q "ResourceInUseException" && echo "⚠️  Table already exists" || echo "✅ Table created"

echo ""
echo "============================================================"
echo "📊 Setup Complete!"
echo "============================================================"
echo ""
echo "Created 3 optimized tables:"
echo "  1. SeasonTrends - All weekly time-series data"
echo "  2. TeamInfo - Team metadata"
echo "  3. Schedule - League schedule"
echo ""
echo "⏳ Tables are being created (this takes 1-2 minutes)"
echo ""
echo "To enable PITR (backups):"
echo "   bash scripts/enable_pitr_v2.sh"
echo ""
echo "============================================================"

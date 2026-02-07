#!/bin/bash
# Enable Point-in-Time Recovery (PITR) for all DynamoDB tables
# Usage: bash scripts/enable_pitr.sh

echo "============================================================"
echo "🛡️  Enabling Point-in-Time Recovery (PITR)"
echo "============================================================"

REGION="us-west-2"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found"
    exit 1
fi

echo ""
echo "📋 Enabling PITR for 5 tables..."
echo ""

# Array of table names
TABLES=(
    "FantasyBaseball-LiveData"
    "FantasyBaseball-WeeklyTimeSeries"
    "FantasyBaseball-MatchupResults"
    "FantasyBaseball-Schedule"
    "FantasyBaseball-AllTimeHistory"
)

SUCCESS_COUNT=0

for TABLE in "${TABLES[@]}"; do
    # Check if PITR is already enabled
    STATUS=$(aws dynamodb describe-continuous-backups \
        --table-name "$TABLE" \
        --region $REGION \
        --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus' \
        --output text 2>/dev/null)

    if [ "$STATUS" == "ENABLED" ]; then
        echo "✅ $TABLE: PITR already enabled"
        ((SUCCESS_COUNT++))
    else
        # Enable PITR
        if aws dynamodb update-continuous-backups \
            --table-name "$TABLE" \
            --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true \
            --region $REGION \
            > /dev/null 2>&1; then
            echo "✅ $TABLE: PITR enabled"
            ((SUCCESS_COUNT++))
        else
            echo "❌ $TABLE: Failed to enable PITR"
        fi
    fi
done

echo ""
echo "============================================================"
echo "✅ Enabled PITR for $SUCCESS_COUNT/${#TABLES[@]} tables"
echo ""
echo "💡 PITR provides:"
echo "   - Continuous backups for the last 35 days"
echo "   - Protection against accidental deletes/updates"
echo "   - Point-in-time restore capability"
echo "============================================================"
echo ""

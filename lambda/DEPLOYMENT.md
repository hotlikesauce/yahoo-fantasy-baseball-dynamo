# Lambda Deployment Guide

This guide covers setting up AWS Lambda functions for automated Yahoo Fantasy data collection.

## Architecture

- **Shared Layer**: `layers/yahoo_fantasy_lib.py` - OAuth, API calls, DynamoDB utilities
- **Lambda Functions**:
  - `pull_live_standings.py` - Every 15 minutes
  - `pull_live_scoreboard.py` - Every 15 minutes
  - `pull_schedule.py` - Manual run (once per season)
  - `pull_weekly_stats.py` - Every Monday 3 AM MST

## Prerequisites

- AWS Account with Lambda, DynamoDB, Secrets Manager access
- Python 3.11+
- AWS CLI configured
- Yahoo Fantasy OAuth credentials (from existing `.env`)

## Step 1: Create Lambda Layer

```bash
# Create layer directory structure
mkdir -p python/lib/python3.11/site-packages

# Copy shared library
cp lambda/layers/yahoo_fantasy_lib.py python/lib/python3.11/site-packages/

# Install dependencies
pip install -r lambda/requirements.txt -t python/lib/python3.11/site-packages/

# Zip layer
zip -r yahoo_fantasy_layer.zip python/

# Upload to AWS Lambda Layers
aws lambda publish-layer-version \
  --layer-name yahoo-fantasy-layer \
  --zip-file fileb://yahoo_fantasy_layer.zip \
  --compatible-runtimes python3.11
```

Note the `LayerVersionArn` returned.

## Step 2: Store Credentials in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name yahoo-fantasy-baseball \
  --secret-string '{
    "YAHOO_CONSUMER_KEY": "YOUR_KEY",
    "YAHOO_CONSUMER_SECRET": "YOUR_SECRET",
    "YAHOO_REFRESH_TOKEN": "YOUR_TOKEN",
    "YAHOO_LEAGUE_ID_2026": "YOUR_LEAGUE_ID"
  }'
```

## Step 3: Create IAM Role for Lambda

```bash
# Create trust policy
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role
aws iam create-role \
  --role-name yahoo-fantasy-lambda-role \
  --assume-role-policy-document file://trust-policy.json

# Attach policies
aws iam attach-role-policy \
  --role-name yahoo-fantasy-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create inline policy for DynamoDB and Secrets Manager
cat > dynamodb-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-west-2:*:table/FantasyBaseball-SeasonTrends",
        "arn:aws:dynamodb:us-west-2:*:table/FantasyBaseball-Schedule"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:us-west-2:*:secret:yahoo-fantasy-baseball*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name yahoo-fantasy-lambda-role \
  --policy-name DynamoDBSecretsAccess \
  --policy-document file://dynamodb-policy.json
```

## Step 4: Create Lambda Functions

### Live Standings (every 15 min)

```bash
# Zip function
cd lambda/functions
zip pull_live_standings.zip pull_live_standings.py

# Create function
aws lambda create-function \
  --function-name pull-live-standings \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/yahoo-fantasy-lambda-role \
  --handler pull_live_standings.lambda_handler \
  --zip-file fileb://pull_live_standings.zip \
  --timeout 60 \
  --memory-size 256 \
  --layers arn:aws:lambda:us-west-2:YOUR_ACCOUNT:layer:yahoo-fantasy-layer:VERSION

# Update function code (for future updates)
aws lambda update-function-code \
  --function-name pull-live-standings \
  --zip-file fileb://pull_live_standings.zip
```

### Live Scoreboard (every 15 min)

```bash
zip pull_live_scoreboard.zip pull_live_scoreboard.py

aws lambda create-function \
  --function-name pull-live-scoreboard \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/yahoo-fantasy-lambda-role \
  --handler pull_live_scoreboard.lambda_handler \
  --zip-file fileb://pull_live_scoreboard.zip \
  --timeout 60 \
  --memory-size 256 \
  --layers arn:aws:lambda:us-west-2:YOUR_ACCOUNT:layer:yahoo-fantasy-layer:VERSION
```

### Schedule (manual - run once)

```bash
zip pull_schedule.zip pull_schedule.py

aws lambda create-function \
  --function-name pull-schedule \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/yahoo-fantasy-lambda-role \
  --handler pull_schedule.lambda_handler \
  --zip-file fileb://pull_schedule.zip \
  --timeout 300 \
  --memory-size 256 \
  --layers arn:aws:lambda:us-west-2:YOUR_ACCOUNT:layer:yahoo-fantasy-layer:VERSION
```

### Weekly Stats (Monday 3 AM MST)

```bash
zip pull_weekly_stats.zip pull_weekly_stats.py

aws lambda create-function \
  --function-name pull-weekly-stats \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/yahoo-fantasy-lambda-role \
  --handler pull_weekly_stats.lambda_handler \
  --zip-file fileb://pull_weekly_stats.zip \
  --timeout 120 \
  --memory-size 256 \
  --layers arn:aws:lambda:us-west-2:YOUR_ACCOUNT:layer:yahoo-fantasy-layer:VERSION
```

## Step 5: Create EventBridge Rules

### Rule 1: Live Standings (every 15 min)

```bash
# Create rule
aws events put-rule \
  --name pull-live-standings-15min \
  --schedule-expression "rate(5 minutes)"

# Add Lambda as target
aws events put-targets \
  --rule pull-live-standings-15min \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-west-2:YOUR_ACCOUNT:function:pull-live-standings"

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
  --function-name pull-live-standings \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-west-2:YOUR_ACCOUNT:rule/pull-live-standings-15min
```

### Rule 2: Live Scoreboard (every 15 min)

```bash
aws events put-rule \
  --name pull-live-scoreboard-15min \
  --schedule-expression "rate(15 minutes)"

aws events put-targets \
  --rule pull-live-scoreboard-15min \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-west-2:YOUR_ACCOUNT:function:pull-live-scoreboard"

aws lambda add-permission \
  --function-name pull-live-scoreboard \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-west-2:YOUR_ACCOUNT:rule/pull-live-scoreboard-15min
```

### Rule 3: Weekly Stats (Monday 3 AM MST = 9 AM UTC)

```bash
# Cron: minute hour day month day-of-week
# 0 9 ? * MON = Monday at 9 AM UTC (3 AM MST)
aws events put-rule \
  --name pull-weekly-stats-monday \
  --schedule-expression "cron(0 9 ? * MON *)"

aws events put-targets \
  --rule pull-weekly-stats-monday \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-west-2:YOUR_ACCOUNT:function:pull-weekly-stats"

aws lambda add-permission \
  --function-name pull-weekly-stats \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-west-2:YOUR_ACCOUNT:rule/pull-weekly-stats-monday
```

## Step 6: Manual Test

```bash
# Test pull-schedule (only needs to run once)
aws lambda invoke \
  --function-name pull-schedule \
  --payload '{}' \
  response.json

cat response.json

# Test live standings
aws lambda invoke \
  --function-name pull-live-standings \
  --payload '{}' \
  response.json

cat response.json
```

## Step 7: Monitor Execution

View logs in CloudWatch:

```bash
# Get recent logs
aws logs tail /aws/lambda/pull-live-standings --follow

# Get logs for specific time range
aws logs filter-log-events \
  --log-group-name /aws/lambda/pull-live-standings \
  --start-time $(date -d '1 hour ago' +%s)000
```

## Updating Functions

After modifying code:

```bash
cd lambda/functions
zip -u pull_live_standings.zip pull_live_standings.py

aws lambda update-function-code \
  --function-name pull-live-standings \
  --zip-file fileb://pull_live_standings.zip
```

## Environment Variables

If needed, you can add environment variables to Lambda functions:

```bash
aws lambda update-function-configuration \
  --function-name pull-live-standings \
  --environment "Variables={LOG_LEVEL=INFO}"
```

## Cleanup

```bash
# Delete rule and targets
aws events remove-targets --rule pull-live-standings-15min --ids 1
aws events delete-rule --name pull-live-standings-15min

# Delete function
aws lambda delete-function --function-name pull-live-standings

# Delete layer
aws lambda delete-layer-version \
  --layer-name yahoo-fantasy-layer \
  --version-number VERSION
```

## Notes

- All times are in UTC for CloudWatch/EventBridge
- 3 AM MST = 9 AM UTC (9 hour offset)
- Functions log to CloudWatch automatically
- DynamoDB auto-scales on-demand, so no provisioning needed
- Secrets are automatically refreshed on each invocation

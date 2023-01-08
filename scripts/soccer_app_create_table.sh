#!/bin/bash

# Creating the stats table
aws dynamodb create-table \
    --table-name player_stats_v1 \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
        ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000 --region us-east-1


# Add gsi1 to stats table to sort response on goals scored
aws dynamodb update-table \
    --table-name player_stats_v1 \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=goals,AttributeType=N \
    --global-secondary-index-updates \
        "[{\"Create\":{\"IndexName\": \"GSI1\",\"KeySchema\":[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"}, {\"AttributeName\":\"goals\",\"KeyType\":\"RANGE\"}], \
        \"ProvisionedThroughput\": {\"ReadCapacityUnits\": 1, \"WriteCapacityUnits\": 1},\"Projection\":{\"ProjectionType\":\"ALL\"}}}]" \
    --endpoint-url http://localhost:8000 --region us-east-1


# Validate table is created
aws dynamodb list-tables --region us-east-1 --endpoint-url http://localhost:8000

# For inserting data into the table
# aws dynamodb put-item --table-name player_stats_v1 --item file://stats_app_insert.json --region us-east-1 --endpoint-url http://localhost:8000

# For cleanup
# aws dynamodb delete-table --table-name player_stats_v1 --region us-east-1 --endpoint-url http://localhost:8000


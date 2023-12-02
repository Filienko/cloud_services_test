#!/bin/bash

# CSV data
csv_data='order_id,product_id,seller_id,date,num_pieces_sold
1,0,0,2022-07-10,62
2,1,1,2022-07-11,45
3,2,0,2022-07-12,30
4,1,2,2022-07-13,75
5,2,0,2022-07-14,20'

# Escape newlines in CSV data
escaped_csv_data=$(echo "$csv_data" | awk '{printf "%s\\n", $0}')

# JSON object to pass to Lambda Function
json='{
  "row": 5,
  "col": 5,
  "bucketname": "projectbucketdatarecieved",
  "filename": "test.csv",
  "CsvData": "'"$escaped_csv_data"'"
}'

echo "Invoking Lambda function using API Gateway"
time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$json" https://ddkxwe33ja.execute-api.us-east-2.amazonaws.com/test)

echo ""
echo ""
echo "JSON RESULT:"
echo "$output" | jq
echo ""


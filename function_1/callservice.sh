#!/bin/bash

# Original CSV data
csv_data='Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
Australia and Oceania,Tuvalu,Baby Food,Offline,H,2022-07-10,669165933,2022-07-15,9925,255.28,159.42,2533654.00,1582243.50,951410.50
Central America and the Caribbean,Grenada,Cereal,Online,C,2022-07-11,963881480,2022-07-18,2804,205.70,117.11,576782.80,328376.44,248406.36
Europe,Russia,Office Supplies,Offline,L,2022-07-12,341417157,2022-07-19,1779,651.21,524.96,1158502.59,933903.84,224598.75
Sub-Saharan Africa,Sao Tome and Principe,Fruits,Online,C,2022-07-13,514321792,2022-07-20,8102,9.33,6.92,75591.66,56065.84,19525.82'

# Escape newlines in CSV data
escaped_csv_data=$(echo "$csv_data" | awk '{printf "%s\\n", $0}')

# JSON object to pass to Lambda Function
json='{
  "bucketname": "projectbucketdatarecieved",
  "filename": "test.csv",
  "data": "'"$escaped_csv_data"'"

}'

echo "Invoking Lambda function using API Gateway"
time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$json" https://ddkxwe33ja.execute-api.us-east-2.amazonaws.com/test)

echo ""
echo "JSON RESULT:"
echo "$output" | jq
echo ""

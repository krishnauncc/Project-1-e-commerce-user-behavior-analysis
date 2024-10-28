#!/usr/bin/env python3
import sys
import csv

def process_user_activity(row):
    # Emit "ProductCategory\tInteraction"
    user_id, activity_type, product_id, activity_timestamp = row[1], row[2], row[3], row[4]
    print(f"{product_id}\tInteraction")

def process_transaction(row):
    # Emit "ProductCategory\tPurchase"
    transaction_id, user_id, product_category, product_id, quantity_sold, revenue_generated, transaction_timestamp = row
    print(f"{product_id}\tPurchase")

for line in sys.stdin:
    line = line.strip()
    if line:  # Skip empty lines
        row = line.split(',')
        if len(row) == 5:  # User activity file
            process_user_activity(row)
        elif len(row) == 7:  # Transaction file
            process_transaction(row)
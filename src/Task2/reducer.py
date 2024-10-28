#!/usr/bin/env python3
import sys
from collections import defaultdict

# Initialize counters for each product category
category_interactions = defaultdict(int)
category_purchases = defaultdict(int)

# Process each line of input
for line in sys.stdin:
    line = line.strip()
    product_id, event_type = line.split('\t')
    
    if event_type == "Interaction":
        category_interactions[product_id] += 1
    elif event_type == "Purchase":
        category_purchases[product_id] += 1

# Output the conversion rate for each product category
print("ProductCategory\tConversionRate")
for category in set(category_interactions.keys()).union(set(category_purchases.keys())):
    interactions = category_interactions[category]
    purchases = category_purchases[category]
    conversion_rate = purchases / interactions if interactions > 0 else 0
    print(f"{category}\t{conversion_rate:.2f}")
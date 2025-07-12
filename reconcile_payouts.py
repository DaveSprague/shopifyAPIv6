# reconcile_payouts.py

import os
import json
import pandas as pd
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION")
USE_CACHE = True

CACHE_FILE = "cached_shopify_orders.json"
PAYOUT_CSV = "payment_transactions_export_1.csv"

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

SHOPIFY_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Load payout transactions CSV
def load_payout_csv(filepath):
    df = pd.read_csv(filepath)
    df['Payout Date'] = pd.to_datetime(df['Payout Date'], utc=True)
    return df

# Extract unique payout dates
def extract_utc_dates(payout_df):
    return sorted(payout_df['Payout Date'].dt.date.unique())

# Build GraphQL query
QUERY = """
query getOrders($cursor: String, $start: DateTime, $end: DateTime) {
  orders(first: 100, after: $cursor, query: "created_at:>=$start created_at:<=$end", reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        name
        createdAt
        totalPriceSet { presentmentMoney { amount } }
        totalTaxSet { presentmentMoney { amount } }
        totalShippingPriceSet { presentmentMoney { amount } }
        totalTipReceivedSet { presentmentMoney { amount } }
        discountApplications(first: 5) {
          edges {
            node {
              ... on DiscountCodeApplication {
                code
                value { ... on MoneyV2 { amount } }
              }
            }
          }
        }
        refunds { createdAt totalRefundedSet { presentmentMoney { amount } } }
        transactions {
          kind
          gateway
          status
          amountSet { presentmentMoney { amount } }
          processedAt
        }
      }
    }
  }
}
"""

# Send paginated GraphQL request
def fetch_orders(start_date, end_date):
    orders = []
    cursor = None
    while True:
        variables = {
            "start": f"{start_date}T00:00:00Z",
            "end": f"{end_date}T23:59:59Z",
            "cursor": cursor
        }
        response = requests.post(SHOPIFY_URL, headers=HEADERS, json={"query": QUERY, "variables": variables})
        data = response.json()
        edges = data['data']['orders']['edges']
        for edge in edges:
            orders.append(edge['node'])
        if not data['data']['orders']['pageInfo']['hasNextPage']:
            break
        cursor = data['data']['orders']['pageInfo']['endCursor']
    return orders

# Flatten order data

def parse_orders(order_data):
    from collections import defaultdict
    by_date = defaultdict(lambda: defaultdict(float))
    trace = defaultdict(list)

    for order in order_data:
        date = pd.to_datetime(order['createdAt']).tz_convert('UTC').date()
        trace[date].append(order['name'])

        by_date[date]['gross_sales'] += float(order['totalPriceSet']['presentmentMoney']['amount'])
        by_date[date]['tax'] += float(order['totalTaxSet']['presentmentMoney']['amount'])
        by_date[date]['shipping'] += float(order['totalShippingPriceSet']['presentmentMoney']['amount'])
        by_date[date]['tips'] += float(order['totalTipReceivedSet']['presentmentMoney']['amount'])

        for disc in order['discountApplications']['edges']:
            by_date[date]['discounts'] += float(disc['node']['value']['amount'])

        for refund in order['refunds']:
            by_date[date]['refunds'] += float(refund['totalRefundedSet']['presentmentMoney']['amount'])

        for txn in order['transactions']:
            if txn['status'] != "SUCCESS":
                continue
            amount = float(txn['amountSet']['presentmentMoney']['amount'])
            gateway = txn['gateway']
            if gateway in ["shopify_payments", "gift_card", "manual", "shopify_payments_cash"]:
                by_date[date][gateway] += amount
            by_date[date]['total_shopify_handled'] += amount if 'shopify' in gateway else 0

    return by_date, trace

# Export to CSV formats
def write_outputs(by_date, trace):
    # Timeseries format
    df_rows = []
    for date, metrics in by_date.items():
        row = {"payout_date": date}
        row.update(metrics)
        df_rows.append(row)
    df = pd.DataFrame(df_rows).sort_values(by="payout_date")
    df.to_csv("daily_summary.csv", index=False)

    # Transposed format
    df_t = df.set_index("payout_date").T
    df_t.to_csv("transposed_summary.csv")

    # Trace for debugging
    with open("debug_trace.json", "w") as f:
        json.dump(trace, f, indent=2, default=str)

# Main execution
if __name__ == "__main__":
    payout_df = load_payout_csv(PAYOUT_CSV)
    payout_dates = extract_utc_dates(payout_df)
    start_date, end_date = payout_dates[0], payout_dates[-1]

    if USE_CACHE and os.path.exists(CACHE_FILE):
        print("Loading from cache...")
        with open(CACHE_FILE, "r") as f:
            order_data = json.load(f)
    else:
        print("Fetching orders from Shopify...")
        order_data = fetch_orders(start_date, end_date)
        with open(CACHE_FILE, "w") as f:
            json.dump(order_data, f, indent=2)

    by_date, trace = parse_orders(order_data)
    write_outputs(by_date, trace)
    print("✅ Reconciliation complete.")

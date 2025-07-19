# analyze_refund_sources.py
# Analyze transaction-level refunds with payment gateway tracking for reconciliation
# Focus on reliable refund data source and actual processing dates

import os
import json
import pandas as pd
import requests
import sys
from datetime import date, datetime, timezone, timedelta
from dotenv import load_dotenv
import hashlib
import pickle
import time
from collections import defaultdict

# Load environment variables
load_dotenv()
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION")

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

SHOPIFY_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Cache configuration
CACHE_DIR = "cache"
CACHE_MAX_AGE_HOURS = 24
ENABLE_CACHE = True

def create_cache_dir():
    """Create cache directory if it doesn't exist"""
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
        print(f"📁 Created cache directory: {CACHE_DIR}")

def get_query_hash(query_string, date_range):
    """Generate hash for GraphQL query and date range to detect changes"""
    combined = f"{query_string}_{date_range[0]}_{date_range[1]}"
    return hashlib.md5(combined.encode()).hexdigest()

def get_cache_filename(query_hash):
    """Generate cache filename based on query hash"""
    return os.path.join(CACHE_DIR, f"refund_analysis_cache_{query_hash}.pkl")

def is_cache_valid(cache_file):
    """Check if cache file exists and is not expired"""
    if not os.path.exists(cache_file):
        return False
    
    cache_age = time.time() - os.path.getmtime(cache_file)
    cache_age_hours = cache_age / 3600
    
    if cache_age_hours > CACHE_MAX_AGE_HOURS:
        print(f"⏰ Cache expired ({cache_age_hours:.1f} hours old, max {CACHE_MAX_AGE_HOURS} hours)")
        return False
    
    print(f"✅ Cache valid ({cache_age_hours:.1f} hours old)")
    return True

def load_from_cache(cache_file):
    """Load orders from cache file"""
    try:
        with open(cache_file, 'rb') as f:
            cached_data = pickle.load(f)
            print(f"📦 Loaded {len(cached_data['orders'])} orders from cache")
            print(f"🕐 Cache created: {cached_data['created_at']}")
            return cached_data['orders']
    except Exception as e:
        print(f"❌ Error loading cache: {e}")
        return None

def save_to_cache(cache_file, orders):
    """Save orders to cache file"""
    try:
        create_cache_dir()
        cached_data = {
            'orders': orders,
            'created_at': datetime.now().isoformat(),
            'count': len(orders)
        }
        with open(cache_file, 'wb') as f:
            pickle.dump(cached_data, f)
        print(f"💾 Saved {len(orders)} orders to cache")
        print(f"📁 Cache file: {cache_file}")
    except Exception as e:
        print(f"❌ Error saving cache: {e}")

def fetch_orders_for_refund_analysis(start_date, end_date):
    """Fetch orders with detailed refund information"""
    
    # Build the query string for date filtering
    date_query = f"created_at:>={start_date}T00:00:00Z AND created_at:<={end_date}T23:59:59Z"

    # Enhanced GraphQL query focusing on refund analysis
    query_with_filter = """
query getOrders($cursor: String, $queryString: String) {
    orders(first: 100, after: $cursor, query: $queryString, reverse: true) {
       pageInfo { hasNextPage endCursor }
        edges {
            node {
                id
                name
                createdAt
                processedAt
                displayFinancialStatus
                totalPriceSet { presentmentMoney { amount currencyCode } }
                totalRefundedSet { presentmentMoney { amount currencyCode } }
                
                # Transaction data including refunds
                transactions {
                    id
                    kind
                    gateway
                    status
                    createdAt
                    processedAt
                    test
                    amountSet { presentmentMoney { amount currencyCode } }
                }
                
                # Detailed refund information
                refunds {
                    id
                    createdAt
                    note
                    refundLineItems(first: 20) {
                        nodes {
                            id
                            quantity
                            subtotalSet {
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                            }
                            totalTaxSet {
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                            }
                        }
                    }
                    transactions(first: 20) {
                        nodes {
                            id
                            kind
                            gateway
                            status
                            createdAt
                            processedAt
                            test
                            amountSet {
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                                presentmentMoney {
                                    amount
                                    currencyCode
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

    # Check cache if enabled
    if ENABLE_CACHE:
        date_range = (start_date, end_date)
        query_hash = get_query_hash(query_with_filter, date_range)
        cache_file = get_cache_filename(query_hash)
        
        print(f"🔍 Checking cache for date range {start_date} to {end_date}")
        print(f"🎯 Query hash: {query_hash}")
        
        if is_cache_valid(cache_file):
            cached_orders = load_from_cache(cache_file)
            if cached_orders is not None:
                print(f"🚀 Using cached data - skipping API calls!")
                return cached_orders
        
        print(f"📡 Cache miss - fetching from Shopify API...")
    else:
        print(f"⚠️ Cache disabled - fetching fresh data from Shopify API...")
    
    # Fetch fresh data from API
    orders = []
    cursor = None
    seen_order_ids = set()
    fetch_count = 0
    order_count = 0
    
    while True:
        fetch_count += 1
        variables = {
            "cursor": cursor,
            "queryString": date_query
        }
        
        print(f"🔄 Fetch #{fetch_count}: Querying orders {'from cursor ' + cursor[:20] + '...' if cursor else 'from start'}")
        response = requests.post(SHOPIFY_URL, headers=HEADERS, json={"query": query_with_filter, "variables": variables})

        if response.status_code != 200:
            raise Exception(f"HTTP Error {response.status_code}: {response.text}")

        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL Error: {json.dumps(data['errors'], indent=2)}")

        edges = data["data"]["orders"]["edges"]
        batch_new_orders = 0
        
        for edge in edges:
            order_id = edge["node"]["id"]
            if order_id not in seen_order_ids:
                seen_order_ids.add(order_id)
                orders.append(edge["node"])
                batch_new_orders += 1
                order_count += 1

        print(f"   📦 Found {batch_new_orders} new orders (total: {order_count})")
        
        if not data["data"]["orders"]["pageInfo"]["hasNextPage"]:
            print(f"✅ Completed fetching {order_count} orders in {fetch_count} API calls")
            break

        cursor = data["data"]["orders"]["pageInfo"]["endCursor"]

    # Save to cache if enabled
    if ENABLE_CACHE:
        save_to_cache(cache_file, orders)

    return orders

def analyze_refund_sources(orders):
    """Analyze transaction-level refunds with payment gateway tracking"""
    
    refund_analysis = []
    summary_stats = {
        'total_orders': 0,
        'orders_with_refunds': 0,
        'gateway_breakdown': defaultdict(lambda: {'count': 0, 'total_amount': 0.0}),
        'refund_timing_stats': {
            'same_day': 0,
            'next_day': 0,
            'within_week': 0,
            'over_week': 0
        }
    }
    
    print(f"🔍 Analyzing transaction refunds for {len(orders)} orders...")
    
    for order in orders:
        order_name = order['name']
        order_created_at = pd.to_datetime(order['createdAt'])
        order_processed_at = pd.to_datetime(order.get('processedAt', order['createdAt']))
        
        summary_stats['total_orders'] += 1
        
        # Focus on transaction-level refunds (the reliable source)
        transaction_refunds = []
        transaction_refund_total = 0.0
        
        for txn in order['transactions']:
            if txn['kind'] == 'REFUND' and txn['status'] == 'SUCCESS':
                refund_amount = float(txn['amountSet']['presentmentMoney']['amount'])
                processed_at = pd.to_datetime(txn['processedAt'])
                
                # Calculate timing relative to order
                timing_days = (processed_at.date() - order_created_at.date()).days
                
                transaction_refunds.append({
                    'id': txn['id'],
                    'amount': refund_amount,
                    'gateway': txn['gateway'],
                    'created_at': pd.to_datetime(txn['createdAt']),
                    'processed_at': processed_at,
                    'timing_days': timing_days
                })
                transaction_refund_total += refund_amount
                
                # Track gateway statistics
                gateway = txn['gateway']
                summary_stats['gateway_breakdown'][gateway]['count'] += 1
                summary_stats['gateway_breakdown'][gateway]['total_amount'] += refund_amount
                
                # Track timing statistics
                if timing_days == 0:
                    summary_stats['refund_timing_stats']['same_day'] += 1
                elif timing_days == 1:
                    summary_stats['refund_timing_stats']['next_day'] += 1
                elif timing_days <= 7:
                    summary_stats['refund_timing_stats']['within_week'] += 1
                else:
                    summary_stats['refund_timing_stats']['over_week'] += 1
        
        # Only record orders that have transaction refunds
        if transaction_refunds:
            summary_stats['orders_with_refunds'] += 1
            
            # Calculate date ranges for this order's refunds
            earliest_processed = min([t['processed_at'] for t in transaction_refunds])
            latest_processed = max([t['processed_at'] for t in transaction_refunds])
            date_spread_days = (latest_processed.date() - earliest_processed.date()).days
            
            # Group refunds by gateway for this order
            gateway_breakdown = defaultdict(lambda: {'count': 0, 'amount': 0.0})
            for refund in transaction_refunds:
                gateway_breakdown[refund['gateway']]['count'] += 1
                gateway_breakdown[refund['gateway']]['amount'] += refund['amount']
            
            refund_analysis.append({
                'order_name': order_name,
                'order_created_at': order_created_at,
                'order_processed_at': order_processed_at,
                
                # Transaction refund summary
                'refund_count': len(transaction_refunds),
                'total_refund_amount': transaction_refund_total,
                'earliest_refund_processed': earliest_processed,
                'latest_refund_processed': latest_processed,
                'refund_date_spread_days': date_spread_days,
                
                # Gateway breakdown for this order
                'gateway_breakdown': dict(gateway_breakdown),
                'primary_gateway': max(gateway_breakdown.keys(), key=lambda g: gateway_breakdown[g]['amount']) if gateway_breakdown else None,
                
                # Timing analysis
                'avg_processing_delay_days': sum([t['timing_days'] for t in transaction_refunds]) / len(transaction_refunds),
                'max_processing_delay_days': max([t['timing_days'] for t in transaction_refunds]),
                'min_processing_delay_days': min([t['timing_days'] for t in transaction_refunds]),
                
                # Raw transaction details for export
                'transaction_refunds_detail': transaction_refunds
            })
    
    return refund_analysis, summary_stats

def generate_refund_summary(refund_analysis, summary_stats):
    """Generate summary focusing on payment gateways and refund timing"""
    
    print(f"\n📊 REFUND ANALYSIS SUMMARY")
    print(f"=" * 50)
    
    print(f"\n📈 OVERALL STATISTICS:")
    print(f"   Total orders analyzed: {summary_stats['total_orders']:,}")
    print(f"   Orders with refunds: {summary_stats['orders_with_refunds']:,}")
    print(f"   Refund rate: {(summary_stats['orders_with_refunds'] / summary_stats['total_orders'] * 100):.2f}%")
    
    print(f"\n� PAYMENT GATEWAY BREAKDOWN:")
    gateway_stats = summary_stats['gateway_breakdown']
    total_refund_amount = sum([stats['total_amount'] for stats in gateway_stats.values()])
    
    for gateway, stats in sorted(gateway_stats.items(), key=lambda x: x[1]['total_amount'], reverse=True):
        percentage = (stats['total_amount'] / total_refund_amount * 100) if total_refund_amount > 0 else 0
        print(f"   {gateway}:")
        print(f"     • Refund count: {stats['count']:,}")
        print(f"     • Total amount: ${stats['total_amount']:,.2f} ({percentage:.1f}%)")
    
    print(f"\n⏰ REFUND TIMING ANALYSIS:")
    timing_stats = summary_stats['refund_timing_stats']
    total_refunds = sum(timing_stats.values())
    
    for timing, count in timing_stats.items():
        percentage = (count / total_refunds * 100) if total_refunds > 0 else 0
        timing_desc = {
            'same_day': 'Same day as order',
            'next_day': 'Next day',
            'within_week': 'Within 1 week',
            'over_week': 'Over 1 week later'
        }
        print(f"   {timing_desc[timing]}: {count:,} ({percentage:.1f}%)")
    
    if refund_analysis:
        df = pd.DataFrame(refund_analysis)
        
        print(f"\n📊 DETAILED TIMING INSIGHTS:")
        
        # Date range analysis
        earliest_refund = df['earliest_refund_processed'].min()
        latest_refund = df['latest_refund_processed'].max()
        if pd.notna(earliest_refund) and pd.notna(latest_refund):
            print(f"   Refund date range: {earliest_refund.date()} to {latest_refund.date()}")
        
        # Processing delay statistics
        avg_delay = df['avg_processing_delay_days'].mean()
        max_delay = df['max_processing_delay_days'].max()
        min_delay = df['min_processing_delay_days'].min()
        
        print(f"   Average processing delay: {avg_delay:.1f} days")
        print(f"   Maximum processing delay: {max_delay} days")
        print(f"   Minimum processing delay: {min_delay} days")
        
        # Amount analysis
        total_refunded = df['total_refund_amount'].sum()
        avg_refund = df['total_refund_amount'].mean()
        
        print(f"\n💰 REFUND AMOUNT ANALYSIS:")
        print(f"   Total refunded: ${total_refunded:,.2f}")
        print(f"   Average refund per order: ${avg_refund:.2f}")
        
        # Gateway usage patterns
        print(f"\n🔍 GATEWAY USAGE PATTERNS:")
        primary_gateways = df['primary_gateway'].value_counts()
        for gateway, count in primary_gateways.items():
            percentage = (count / len(df) * 100)
            print(f"   {gateway}: {count:,} orders ({percentage:.1f}%)")
        
        # Export files with gateway focus
        output_file = "refund_gateway_analysis.csv"
        
        # Flatten gateway breakdown for CSV export
        export_data = []
        for _, row in df.iterrows():
            base_row = {
                'order_name': row['order_name'],
                'order_created_date': row['order_created_at'].date(),
                'order_processed_date': row['order_processed_at'].date(),
                'refund_count': row['refund_count'],
                'total_refund_amount': row['total_refund_amount'],
                'earliest_refund_date': row['earliest_refund_processed'].date(),
                'latest_refund_date': row['latest_refund_processed'].date(),
                'refund_date_spread_days': row['refund_date_spread_days'],
                'avg_processing_delay_days': row['avg_processing_delay_days'],
                'max_processing_delay_days': row['max_processing_delay_days'],
                'primary_gateway': row['primary_gateway']
            }
            
            # Add gateway breakdown columns
            for gateway, details in row['gateway_breakdown'].items():
                base_row[f'{gateway}_count'] = details['count']
                base_row[f'{gateway}_amount'] = details['amount']
            
            export_data.append(base_row)
        
        export_df = pd.DataFrame(export_data)
        export_df.to_csv(output_file, index=False)
        print(f"\n💾 Gateway analysis saved to: {output_file}")
        
        # Create detailed transaction export
        transaction_details = []
        for _, row in df.iterrows():
            for txn in row['transaction_refunds_detail']:
                transaction_details.append({
                    'order_name': row['order_name'],
                    'order_created_date': row['order_created_at'].date(),
                    'transaction_id': txn['id'],
                    'refund_amount': txn['amount'],
                    'payment_gateway': txn['gateway'],
                    'refund_created_date': txn['created_at'].date(),
                    'refund_processed_date': txn['processed_at'].date(),
                    'processing_delay_days': txn['timing_days']
                })
        
        transaction_df = pd.DataFrame(transaction_details)
        transaction_file = "refund_transactions_by_gateway.csv"
        transaction_df.to_csv(transaction_file, index=False)
        print(f"💾 Transaction details saved to: {transaction_file}")
        
        # Gateway reconciliation recommendations
        print(f"\n🎯 RECONCILIATION RECOMMENDATIONS:")
        print(f"   ✅ Use transaction refunds as primary data source")
        print(f"   📅 Group refunds by actual processed dates, not order dates")
        print(f"   💳 Track refunds separately by payment gateway:")
        
        for gateway in sorted(gateway_stats.keys()):
            gateway_orders = df[df['primary_gateway'] == gateway]
            if len(gateway_orders) > 0:
                avg_delay = gateway_orders['avg_processing_delay_days'].mean()
                print(f"      • {gateway}: {len(gateway_orders)} orders, avg {avg_delay:.1f} day delay")
    
    return df if refund_analysis else None

def main():
    print(f"🔍 SHOPIFY REFUND GATEWAY ANALYSIS")
    print(f"=" * 50)
    
    # Calculate date range: January 1, 2025 to two days ago
    end_date = (datetime.now() - timedelta(days=2)).date()
    start_date = date(2025, 1, 1)
    
    print(f"📅 Analysis period: {start_date} to {end_date}")
    print(f"📊 Analyzing refund transactions and payment gateways:")
    print(f"   • Focus on transaction-level refunds (most reliable source)")
    print(f"   • Track payment gateway for each refund")
    print(f"   • Use actual refund processing dates for accurate cash flow")
    print()
    
    # Fetch orders
    print(f"🛒 Fetching orders from Shopify...")
    orders = fetch_orders_for_refund_analysis(start_date, end_date)
    
    if not orders:
        print(f"❌ No orders found for the specified date range")
        return
    
    # Analyze refund sources
    print(f"\n🔍 Analyzing refund sources...")
    refund_analysis, summary_stats = analyze_refund_sources(orders)
    
    # Generate summary
    df = generate_refund_summary(refund_analysis, summary_stats)
    
    print(f"\n✅ Analysis complete!")

if __name__ == "__main__":
    main()

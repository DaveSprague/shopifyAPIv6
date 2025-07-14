# reconcile_payouts.py

import os
import json
import pandas as pd
import requests
import sys
from datetime import date, datetime, timezone
from dotenv import load_dotenv
import hashlib
import pickle
import time

# Load environment variables
load_dotenv()
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION")

SALES_TIMEZONE = "US/Eastern"  # Set your timezone for sales data

# Payout CSV configuration
PAYOUT_FOLDER = "payoutTransactionFiles"
PAYOUT_CSV = "recent_payment_transactions_export_1.csv"  # Default fallback

# Cache configuration
CACHE_DIR = "cache"
CACHE_MAX_AGE_HOURS = 24  # Cache expires after 24 hours
ENABLE_CACHE = True  # Set to False to disable caching

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

SHOPIFY_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

def create_cache_dir():
    """Create cache directory if it doesn't exist"""
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
        print(f"📁 Created cache directory: {CACHE_DIR}")

def select_payout_csv_file():
    """Allow user to select a CSV file from the payoutTransactionFiles folder"""
    
    # Check if payoutTransactionFiles folder exists
    if not os.path.exists(PAYOUT_FOLDER):
        print(f"❌ Folder '{PAYOUT_FOLDER}' not found!")
        print(f"💡 Please create the folder and place your payout CSV files there")
        return None
    
    # Get list of CSV files in the folder
    csv_files = [f for f in os.listdir(PAYOUT_FOLDER) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print(f"❌ No CSV files found in '{PAYOUT_FOLDER}' folder!")
        print(f"💡 Please place your payout transaction CSV files in the '{PAYOUT_FOLDER}' folder")
        return None
    
    # Sort files by modification time (newest first) for convenience
    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(PAYOUT_FOLDER, x)), reverse=True)
    
    print(f"\n📂 SELECT PAYOUT CSV FILE")
    print(f"=" * 40)
    print(f"Found {len(csv_files)} CSV files in '{PAYOUT_FOLDER}' folder:\n")
    
    for i, filename in enumerate(csv_files, 1):
        filepath = os.path.join(PAYOUT_FOLDER, filename)
        # Get file size and modification time
        file_size = os.path.getsize(filepath)
        file_size_mb = file_size / (1024 * 1024)
        mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        
        print(f"{i}. {filename}")
        print(f"   Size: {file_size_mb:.1f} MB")
        print(f"   Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    # Let user select file
    while True:
        try:
            choice = input(f"Enter your choice (1-{len(csv_files)}): ").strip()
            
            if choice.isdigit():
                choice_num = int(choice)
                if 1 <= choice_num <= len(csv_files):
                    selected_file = csv_files[choice_num - 1]
                    selected_path = os.path.join(PAYOUT_FOLDER, selected_file)
                    
                    print(f"\n✅ Selected: {selected_file}")
                    print(f"📁 Full path: {selected_path}")
                    
                    return selected_path
                else:
                    print(f"❌ Please enter a number between 1 and {len(csv_files)}")
            else:
                print("❌ Please enter a valid number")
                
        except KeyboardInterrupt:
            print("\n🚪 Cancelled by user")
            return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

def get_query_hash(query_string, date_range):
    """Generate hash for GraphQL query and date range to detect changes"""
    # Combine query and date range for hashing
    combined = f"{query_string}_{date_range[0]}_{date_range[1]}"
    return hashlib.md5(combined.encode()).hexdigest()

def get_cache_filename(query_hash):
    """Generate cache filename based on query hash"""
    return os.path.join(CACHE_DIR, f"orders_cache_{query_hash}.pkl")

def is_cache_valid(cache_file):
    """Check if cache file exists and is not expired"""
    if not os.path.exists(cache_file):
        return False
    
    # Check cache age
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

def clear_cache():
    """Clear all cache files"""
    if os.path.exists(CACHE_DIR):
        import shutil
        shutil.rmtree(CACHE_DIR)
        print(f"🗑️ Cleared cache directory: {CACHE_DIR}")

def show_cache_status():
    """Show current cache status and files"""
    print("\n📁 CACHE STATUS")
    print("=" * 40)
    
    if not ENABLE_CACHE:
        print("❌ Cache is disabled")
        return
    
    if not os.path.exists(CACHE_DIR):
        print("📁 No cache directory found")
        return
    
    cache_files = [f for f in os.listdir(CACHE_DIR) if f.startswith('orders_cache_') and f.endswith('.pkl')]
    
    if not cache_files:
        print("📁 Cache directory is empty")
        return
    
    print(f"📁 Cache directory: {CACHE_DIR}")
    print(f"📦 Found {len(cache_files)} cache files:")
    
    for cache_file in cache_files:
        cache_path = os.path.join(CACHE_DIR, cache_file)
        cache_age = time.time() - os.path.getmtime(cache_path)
        cache_age_hours = cache_age / 3600
        
        # Extract query hash from filename
        query_hash = cache_file.replace('orders_cache_', '').replace('.pkl', '')
        
        status = "✅ VALID" if cache_age_hours <= CACHE_MAX_AGE_HOURS else "⏰ EXPIRED"
        print(f"   • {cache_file}")
        print(f"     Hash: {query_hash}")
        print(f"     Age: {cache_age_hours:.1f} hours - {status}")
        
        # Try to load cache info
        try:
            with open(cache_path, 'rb') as f:
                cached_data = pickle.load(f)
                print(f"     Orders: {cached_data.get('count', 'unknown')}")
                print(f"     Created: {cached_data.get('created_at', 'unknown')}")
        except:
            print(f"     Error reading cache file")
        print()

def manage_cache():
    """Interactive cache management"""
    global ENABLE_CACHE
    
    while True:
        print("\n🗂️  CACHE MANAGEMENT")
        print("=" * 30)
        print("1. Show cache status")
        print("2. Clear all cache")
        print("3. Toggle cache (currently {})".format("ON" if ENABLE_CACHE else "OFF"))
        print("4. Return to main menu")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            show_cache_status()
        elif choice == "2":
            confirm = input("Are you sure you want to clear all cache? (y/N): ").strip().lower()
            if confirm == 'y':
                clear_cache()
            else:
                print("❌ Cache clear cancelled")
        elif choice == "3":
            ENABLE_CACHE = not ENABLE_CACHE
            print(f"🔄 Cache is now {'ON' if ENABLE_CACHE else 'OFF'}")
        elif choice == "4":
            break
        else:
            print("❌ Invalid choice")

# Load payout transactions CSV
def load_payout_csv(filepath):
    df = pd.read_csv(filepath)
    df['Payout Date'] = pd.to_datetime(df['Payout Date'], utc=True)
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce', utc=True)
    df['Payout Status'] = df.get('Payout Status', 'unknown').fillna('unknown')
    df['Payout Amount'] = pd.to_numeric(df.get('Amount', 0), errors='coerce').fillna(0.0)
    df['Payout Fee'] = pd.to_numeric(df.get('Fee', 0), errors='coerce').fillna(0.0)
    df['Payout Net Deposit'] = pd.to_numeric(df.get('Net', 0), errors='coerce').fillna(0.0)
    df['Type'] = df.get('Type', 'other').fillna('other')
    return df

# Extract date range based on all payout transactions
def extract_utc_date_range(payout_df):
    min_date = payout_df['Transaction Date'].min().date()
    max_date = payout_df['Transaction Date'].max().date()
    return min_date, max_date

# Send paginated GraphQL request

def fetch_orders(start_date, end_date):
    """Fetch orders with caching support"""
    
    # Build the query string for date filtering
    date_query = f"created_at:>={start_date}T00:00:00Z AND created_at:<={end_date}T23:59:59Z"

    # Simplified GraphQL query focusing on essential reconciliation data
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
                displayFulfillmentStatus
                
                # Essential financial totals for reconciliation
                totalPriceSet { presentmentMoney { amount currencyCode } }
                totalTaxSet { presentmentMoney { amount currencyCode } }
                totalShippingPriceSet { presentmentMoney { amount currencyCode } }
                totalRefundedSet { presentmentMoney { amount currencyCode } }
                totalTipReceivedSet { presentmentMoney { amount currencyCode } }
                totalReceivedSet { presentmentMoney { amount currencyCode } }
                totalDiscountsSet { presentmentMoney { amount currencyCode } }
                netPaymentSet { presentmentMoney { amount currencyCode } }
                subtotalPriceSet { presentmentMoney { amount currencyCode } }
                totalOutstandingSet { presentmentMoney { amount currencyCode } }
                
                # Essential transaction data for payment reconciliation
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
                
                # Basic refund information
                refunds {
                    id
                    createdAt
                    totalRefundedSet { presentmentMoney { amount currencyCode } }
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
    order_count = 0  # Initialize order counter
    
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

from collections import defaultdict

def parse_orders(order_data):
    # Create data structures for both UTC and shop timezone
    by_date_utc = defaultdict(lambda: defaultdict(float))
    by_date_shop = defaultdict(lambda: defaultdict(float))
    trace_utc = defaultdict(list)
    trace_shop = defaultdict(list)
    detailed_transactions_utc = defaultdict(list)
    detailed_transactions_shop = defaultdict(list)

    for order in order_data:
        # Calculate dates in both timezones
        created_utc = pd.to_datetime(order['createdAt']).tz_convert('UTC').date()
        created_shop = pd.to_datetime(order['createdAt']).tz_convert(SALES_TIMEZONE).date()
        processed_utc = pd.to_datetime(order.get('processedAt', order['createdAt'])).tz_convert('UTC').date()
        processed_shop = pd.to_datetime(order.get('processedAt', order['createdAt'])).tz_convert(SALES_TIMEZONE).date()
        
        date_str_utc = str(created_utc)
        date_str_shop = str(created_shop)
        
        # Store trace data for both timezones
        order_trace = {
            'order_name': order['name'],
            'financial_status': order.get('displayFinancialStatus'),
            'fulfillment_status': order.get('displayFulfillmentStatus'),
            'created_utc': str(created_utc),
            'created_shop': str(created_shop)
        }
        trace_utc[date_str_utc].append(order_trace)
        trace_shop[date_str_shop].append(order_trace)

        # Basic order totals - all essential fields are guaranteed to exist
        subtotal = float(order['subtotalPriceSet']['presentmentMoney']['amount'])
        discounts = float(order['totalDiscountsSet']['presentmentMoney']['amount'])
        gross_sales = discounts + subtotal
        tax = float(order['totalTaxSet']['presentmentMoney']['amount'])
        shipping = float(order['totalShippingPriceSet']['presentmentMoney']['amount'])
        tips = float(order['totalTipReceivedSet']['presentmentMoney']['amount'])
        net_payment = float(order['netPaymentSet']['presentmentMoney']['amount'])
        net_payment_chk = gross_sales + tax + shipping - discounts
        refunds = float(order['totalRefundedSet']['presentmentMoney']['amount'])
        outstanding = float(order['totalOutstandingSet']['presentmentMoney']['amount'])
        total_received = float(order['totalReceivedSet']['presentmentMoney']['amount'])
        total_received_chk = net_payment_chk + refunds

        # Aggregate by creation date for both timezones
        for by_date, date_str in [(by_date_utc, date_str_utc), (by_date_shop, date_str_shop)]:
            by_date[date_str]['gross_sales'] += gross_sales
            by_date[date_str]['subtotal'] += subtotal
            by_date[date_str]['discounts'] += discounts
            by_date[date_str]['tax'] += tax
            by_date[date_str]['shipping'] += shipping
            by_date[date_str]['tips'] += tips
            by_date[date_str]['refunds'] += refunds
            by_date[date_str]['net_payment'] += net_payment
            by_date[date_str]['net_payment_chk'] += net_payment_chk
            by_date[date_str]['outstanding'] += outstanding
            by_date[date_str]['total_received'] += total_received
            by_date[date_str]['total_received_chk'] += total_received_chk
            by_date[date_str]['order_count'] += 1

        # Process transactions for payment tracking in both timezones
        for txn in order['transactions']:
            if txn['status'] != "SUCCESS":
                continue
                
            txn_date_utc = pd.to_datetime(txn['processedAt']).tz_convert('UTC').date()
            txn_date_shop = pd.to_datetime(txn['processedAt']).tz_convert(SALES_TIMEZONE).date()
            txn_date_str_utc = str(txn_date_utc)
            txn_date_str_shop = str(txn_date_shop)
            
            amount = float(txn['amountSet']['presentmentMoney']['amount'])
            gateway = txn['gateway']
            kind = txn['kind']
            
            # Process for both timezones
            for by_date, txn_date_str, detailed_transactions in [
                (by_date_utc, txn_date_str_utc, detailed_transactions_utc),
                (by_date_shop, txn_date_str_shop, detailed_transactions_shop)
            ]:
                # Track by transaction date (when money actually moved)
                by_date[txn_date_str][f'{gateway}_{kind.lower()}'] += amount
                
                # For Shopify Payments, only count AUTHORIZATION transactions to avoid double-counting
                # CAPTURE transactions happen after AUTHORIZATION and represent the same money movement
                if 'shopify' in gateway.lower() and kind.upper() == 'AUTHORIZATION':
                    by_date[txn_date_str]['shopify_payments'] += amount
                # For other gateways, count SALE transactions (immediate capture)
                elif 'shopify' not in gateway.lower() and kind.upper() == 'SALE':
                    if 'gift' in gateway.lower():
                        by_date[txn_date_str]['gift_card'] += amount
                    elif 'manual' in gateway.lower() or 'cash' in gateway.lower():
                        by_date[txn_date_str]['cash'] += amount
                    else:
                        by_date[txn_date_str]['manual'] += amount
                # Count refunds separately regardless of gateway
                elif kind.upper() == 'REFUND':
                    by_date[txn_date_str][f'{gateway}_refunds'] += amount
                
                # Store detailed transaction info
                detailed_transactions[txn_date_str].append({
                    'order_name': order['name'],
                    'transaction_id': txn['id'],
                    'kind': kind,
                    'gateway': gateway,
                    'amount': amount,
                    'test': txn.get('test', False),
                    'processed_utc': str(txn_date_utc),
                    'processed_shop': str(txn_date_shop)
                })

        # Process basic refund information for both timezones
        for refund in order.get('refunds', []):
            refund_date_utc = pd.to_datetime(refund['createdAt']).tz_convert('UTC').date()
            refund_date_shop = pd.to_datetime(refund['createdAt']).tz_convert(SALES_TIMEZONE).date()
            refund_date_str_utc = str(refund_date_utc)
            refund_date_str_shop = str(refund_date_shop)
            refund_amount = float(refund['totalRefundedSet']['presentmentMoney']['amount'])
            
            by_date_utc[refund_date_str_utc]['refund_amount'] += refund_amount
            by_date_shop[refund_date_str_shop]['refund_amount'] += refund_amount

        # Calculate reconciliation metrics for both timezones
        for by_date, date_str in [(by_date_utc, date_str_utc), (by_date_shop, date_str_shop)]:
            by_date[date_str]['calculated_total'] = subtotal + tax + shipping + tips - discounts

    return (by_date_utc, trace_utc, detailed_transactions_utc, 
            by_date_shop, trace_shop, detailed_transactions_shop)

def write_outputs(by_date_utc, trace_utc, detailed_transactions_utc, 
                  by_date_shop, trace_shop, detailed_transactions_shop, payout_df):
    
    # Generate UTC-aligned data (for payout reconciliation)
    print("   📊 Generating UTC-aligned reconciliation data...")
    df_utc = generate_reconciliation_dataframe(by_date_utc, detailed_transactions_utc, payout_df, "UTC")
    print("   💾 Writing daily_sales_reconciliation_utc.csv...")
    df_utc.to_csv("daily_sales_reconciliation_utc.csv", index=False)
    
    # Create transposed view for UTC data
    print("   📋 Creating transposed UTC view...")
    df_utc_t = df_utc.set_index("date").T
    df_utc_t.to_csv("transposed_reconciliation_utc.csv")
    
    # Export UTC trace data
    print("   📝 Writing UTC trace data...")
    with open("order_trace_utc.json", "w") as f:
        json.dump(trace_utc, f, indent=2, default=str)
    
    # Create detailed transaction export for UTC
    detailed_rows_utc = []
    for date_str, transactions in detailed_transactions_utc.items():
        for txn in transactions:
            txn['date'] = date_str
            detailed_rows_utc.append(txn)
    
    if detailed_rows_utc:
        print("   📄 Writing detailed UTC transactions...")
        detailed_df_utc = pd.DataFrame(detailed_rows_utc)
        detailed_df_utc.to_csv("detailed_transactions_utc.csv", index=False)
    
    # Generate Shop timezone-aligned data (for debugging against Shopify reports)
    print(f"   📊 Generating {SALES_TIMEZONE}-aligned data for Shopify report comparison...")
    print(f"   🕐 NOTE: Payout transactions will be regrouped by {SALES_TIMEZONE} dates")
    print(f"   ⚠️  Fee and Net amounts will be recalculated based on regrouped charges")
    df_shop = generate_reconciliation_dataframe(by_date_shop, detailed_transactions_shop, payout_df, SALES_TIMEZONE)
    print("   💾 Writing daily_sales_reconciliation_shop_timezone.csv...")
    df_shop.to_csv("daily_sales_reconciliation_shop_timezone.csv", index=False)
    
    # Create transposed view for shop timezone data
    print("   📋 Creating transposed shop timezone view...")
    df_shop_t = df_shop.set_index("date").T
    df_shop_t.to_csv("transposed_reconciliation_shop_timezone.csv")
    
    # Export shop timezone trace data
    print(f"   📝 Writing {SALES_TIMEZONE} trace data...")
    with open("order_trace_shop_timezone.json", "w") as f:
        json.dump(trace_shop, f, indent=2, default=str)
    
    # Create detailed transaction export for shop timezone
    print(f"   📊 Writing {SALES_TIMEZONE} detailed transactions...")
    detailed_rows_shop = []
    for date_str, transactions in detailed_transactions_shop.items():
        for txn in transactions:
            txn['date'] = date_str
            detailed_rows_shop.append(txn)
    
    if detailed_rows_shop:
        detailed_df_shop = pd.DataFrame(detailed_rows_shop)
        detailed_df_shop.to_csv("detailed_transactions_shop_timezone.csv", index=False)
    
    # Also maintain backwards compatibility with original files (using UTC for payout reconciliation)
    print("   📝 Writing UTC backward compatibility files...")
    df_utc.to_csv("daily_sales_reconciliation_fixed.csv", index=False)
    df_utc_t.to_csv("transposed_reconciliation_fixed.csv")
    
    print(f"\n✅ RECONCILIATION COMPLETE! Files generated:")
    print(f"   📊 UTC (for accurate payout reconciliation):")
    print(f"     • daily_sales_reconciliation_utc.csv")
    print(f"     • transposed_reconciliation_utc.csv")
    print(f"     • detailed_transactions_utc.csv")
    print(f"     • order_trace_utc.json")
    print(f"   📊 {SALES_TIMEZONE} (for Shopify report comparison):")
    print(f"     • daily_sales_reconciliation_shop_timezone.csv")
    print(f"     • transposed_reconciliation_shop_timezone.csv")
    print(f"     • detailed_transactions_shop_timezone.csv")
    print(f"     • order_trace_shop_timezone.json")
    print(f"   ")
    print(f"   🕐 TIMEZONE DIFFERENCES:")
    print(f"   • UTC files: Sales, payments, and payouts all in UTC timezone")
    print(f"   • {SALES_TIMEZONE} files: Sales and payments in {SALES_TIMEZONE}, payouts regrouped to {SALES_TIMEZONE}")
    print(f"   • For accurate reconciliation: Use UTC files")
    print(f"   • For Shopify report comparison: Use {SALES_TIMEZONE} files")
    print(f"\n📊 Run beautiful_mismatch_viewer.py to view Excel analysis!")
    
    return df_utc, df_shop

def generate_reconciliation_dataframe(by_date, detailed_transactions, payout_df, timezone_name):
    """Generate reconciliation dataframe for a specific timezone"""
    df_rows = []
    
    # Convert payout dates to the specified timezone if not UTC
    if timezone_name != "UTC":
        print(f"   🕐 Converting payout transaction dates from UTC to {timezone_name} timezone...")
        payout_df_tz = payout_df.copy()
        # Convert Transaction Date to the specified timezone
        payout_df_tz['Transaction Date'] = payout_df_tz['Transaction Date'].dt.tz_convert(timezone_name)
        
        # Also convert Payout Date for completeness
        payout_df_tz['Payout Date'] = payout_df_tz['Payout Date'].dt.tz_convert(timezone_name)
        
        print(f"   📊 Payout dates converted: Using {timezone_name} timezone for grouping")
    else:
        payout_df_tz = payout_df  # Use original UTC dates
    
    # Get all unique dates from both orders and payouts
    all_dates = set(by_date.keys())
    payout_dates = set(payout_df_tz['Transaction Date'].dt.date.astype(str))
    all_dates.update(payout_dates)
    
    for date_str in sorted(all_dates):
        metrics = by_date.get(date_str, defaultdict(float))
        row = {"date": date_str, "timezone": timezone_name}
        
        # Order metrics
        row.update({
            'order_count': metrics.get('order_count', 0),
            'gross_sales': metrics.get('gross_sales', 0),
            'subtotal': metrics.get('subtotal', 0),
            'discounts': metrics.get('discounts', 0),
            'tax': metrics.get('tax', 0),
            'shipping': metrics.get('shipping', 0),
            'tips': metrics.get('tips', 0),
            'refunds': metrics.get('refunds', 0),
            'net_payment': metrics.get('net_payment', 0),
            'outstanding': metrics.get('outstanding', 0),
            'total_received': metrics.get('total_received', 0),
            'calculated_total': metrics.get('calculated_total', 0),
        })
        
        # Payment processing metrics
        row.update({
            'shopify_payments': metrics.get('shopify_payments', 0),
            'gift_card': metrics.get('gift_card', 0),
            'cash': metrics.get('cash', 0),
            'manual': metrics.get('manual', 0),
            'total_shopify_handled': metrics.get('shopify_payments', 0),  # Only authorized Shopify payments
        })
        
        # Payout data reconciliation using timezone-converted dates
        date_obj = pd.to_datetime(date_str).date()
        payout_day = payout_df_tz[payout_df_tz['Transaction Date'].dt.date == date_obj]
        
        # Filter out pending payouts (only include 'paid' transactions)
        payout_day_paid = payout_day[payout_day['Payout Status'] == 'paid']
        
        # Separate payout data by type (only for paid transactions)
        payout_charges = payout_day_paid[payout_day_paid['Type'] == 'charge']
        payout_refunds = payout_day_paid[payout_day_paid['Type'] == 'refund']
        payout_adjustments = payout_day_paid[payout_day_paid['Type'] == 'adjustment']
        payout_chargebacks = payout_day_paid[payout_day_paid['Type'] == 'chargeback']
        payout_chargebacks_won = payout_day_paid[payout_day_paid['Type'] == 'chargeback won']
        payout_shop_cash_credit = payout_day_paid[payout_day_paid['Type'] == 'shop_cash_credit']    
        payout_other = payout_day_paid[~payout_day_paid['Type'].isin(['charge', 'refund', 'adjustment', 'chargeback', 'chargeback won', 'shop_cash_credit'])]
        


        payout_amount = payout_charges['Payout Amount'].sum()
        payout_fee = payout_day_paid['Payout Fee'].sum()
        payout_net_deposit = payout_day_paid['Payout Net Deposit'].sum()

        
        row.update({
            'payout_amount': payout_amount,
            'payout_fee': payout_fee,
            'payout_net_deposit': payout_net_deposit,
            'payout_count': len(payout_day_paid),
            'payout_statuses': ", ".join(sorted(payout_day_paid['Payout Status'].unique())) if len(payout_day_paid) > 0 else "",
            'payout_types': ", ".join(sorted(payout_day_paid['Type'].unique())) if len(payout_day_paid) > 0 else "",
            'payout_type_adjustment': payout_adjustments['Payout Amount'].sum(),
            'payout_type_chargeback': payout_chargebacks['Payout Amount'].sum(),
            'payout_type_chargeback_won': payout_chargebacks_won['Payout Amount'].sum(),
            'payout_type_shop_cash_credit': payout_shop_cash_credit['Payout Amount'].sum(),
            'payout_type_refund': payout_refunds['Payout Amount'].sum(),
            'payout_type_payout': payout_other[payout_other['Type'] == 'payout']['Payout Amount'].sum(),
            'payout_type_other': payout_other[payout_other['Type'] != 'payout']['Payout Amount'].sum(),
            # Track pending transactions separately for visibility
            'pending_payout_amount': payout_day[payout_day['Payout Status'] != 'paid']['Payout Amount'].sum(),
            'pending_payout_count': len(payout_day[payout_day['Payout Status'] != 'paid']),
        })
        
        # Reconciliation analysis - compare with payout data for fee calculation (only paid transactions)
        shopify_handled = row['shopify_payments']  # Only authorized Shopify payments
        payout_amount = row['payout_amount']  # Charges grouped by the specified timezone

        #  TODO: USING ABS is a bit dangerous here!!!
        payout_refunds_amount = abs(row['payout_type_refund'])  # Refunds from payout data (make positive)
        payout_fee = row['payout_fee']  # Fees from payout data (may be invalid if timezone regrouped)
        payout_net_deposit = row['payout_net_deposit']
    

        # For proper reconciliation, we need to account for the fact that refunds were originally payments
        # Total Shopify receipts = shopify_payments + refunds (since refunds were originally received as payments)
        total_shopify_receipts = shopify_handled + payout_refunds_amount
        expected_payout_before_fees = payout_amount  # This is the payout charges before fees
        
        # Check if amounts match (within 1 cent tolerance) - only for days with paid payouts
        mismatch = abs(total_shopify_receipts - expected_payout_before_fees) > 0.01 if expected_payout_before_fees > 0 else False
        
        row.update({
            'reconciliation_difference': total_shopify_receipts - expected_payout_before_fees,
            'mismatch': mismatch,
            'reconciliation_percentage': (expected_payout_before_fees / total_shopify_receipts * 100) if total_shopify_receipts != 0 else 0,
            'calculated_fees_from_payout': payout_fee,
            'total_shopify_receipts': total_shopify_receipts,  # Add this for transparency
            'expected_payout_before_fees': expected_payout_before_fees,  # Add this for transparency
            'timezone_conversion_note': f"Payout dates converted to {timezone_name}" if timezone_name != "UTC" else "Original UTC dates"
        })
        
        df_rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(df_rows).sort_values(by="date")
    return df

# Main execution
if __name__ == "__main__":
    print("🚀 SHOPIFY PAYMENT RECONCILIATION")
    print("=" * 50)
    
    # Check for cache management arguments
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "--cache":
            manage_cache()
            sys.exit(0)
        elif sys.argv[1] == "--clear-cache":
            clear_cache()
            sys.exit(0)
        elif sys.argv[1] == "--cache-status":
            show_cache_status()
            sys.exit(0)
        elif sys.argv[1] == "--no-cache":
            ENABLE_CACHE = False
            print("⚠️  Cache disabled for this run")
    
    # Show cache status
    if ENABLE_CACHE:
        print(f"📁 Cache: ENABLED (max age: {CACHE_MAX_AGE_HOURS} hours)")
        if os.path.exists(CACHE_DIR):
            cache_files = [f for f in os.listdir(CACHE_DIR) if f.startswith('orders_cache_')]
            if cache_files:
                print(f"   📦 {len(cache_files)} cache files found")
        print("   💡 Use --cache for management, --clear-cache to clear, --no-cache to disable")
    else:
        print("📁 Cache: DISABLED")
    print()
    
    # Select payout CSV file
    selected_payout_file = select_payout_csv_file()
    if selected_payout_file is None:
        print("❌ No payout file selected. Exiting.")
        sys.exit(1)
    
    print("\n📂 Loading payout CSV file...")
    payout_df = load_payout_csv(selected_payout_file)
    print(f"   ✅ Loaded {len(payout_df)} payout transactions")
    
    start_date, end_date = extract_utc_date_range(payout_df)
    print(f"📅 Reconciliation period: {start_date} to {end_date}")

    print("\n🛒 Fetching orders from Shopify GraphQL API...")
    order_data = fetch_orders(start_date, end_date)
    print(f"   ✅ Retrieved {len(order_data)} orders total")

    print("\n🔄 Processing orders and calculating daily summaries...")
    (by_date_utc, trace_utc, detailed_transactions_utc, 
     by_date_shop, trace_shop, detailed_transactions_shop) = parse_orders(order_data)
    
    print(f"   ✅ Processed orders for UTC timezone")
    print(f"   ✅ Processed orders for {SALES_TIMEZONE} timezone")
    
    print("\n💾 Writing output files and reconciling with payouts...")
    df_utc, df_shop = write_outputs(by_date_utc, trace_utc, detailed_transactions_utc,
                                   by_date_shop, trace_shop, detailed_transactions_shop, payout_df)
    
    print(f"\n📊 UTC Daily Summary Preview (for payout reconciliation):")
    print(df_utc.head(10).to_string(index=False))
    
    print(f"\n📊 {SALES_TIMEZONE} Daily Summary Preview (for Shopify report comparison):")
    print(df_shop.head(10).to_string(index=False))
    
    # Show timezone impact summary
    utc_total = df_utc['gross_sales'].sum()
    shop_total = df_shop['gross_sales'].sum()
    print(f"\n🌍 Timezone Impact Summary:")
    print(f"Total sales (UTC grouping): ${utc_total:,.2f}")
    print(f"Total sales ({SALES_TIMEZONE} grouping): ${shop_total:,.2f}")
    print(f"Difference: ${shop_total - utc_total:,.2f}")
    
    # Count mismatches in both datasets
    utc_mismatches = df_utc['mismatch'].sum()
    shop_mismatches = df_shop['mismatch'].sum()
    print(f"\nMismatches in UTC data: {utc_mismatches}")
    print(f"Mismatches in {SALES_TIMEZONE} data: {shop_mismatches}")
    
    print("✅ Dual-timezone reconciliation complete.")

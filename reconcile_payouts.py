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

# TODO: the sales metrics still aren't matching up quite correctly.  Gross and Net Sales calculations need to be reviewed.
# TODO: Add some more recinciliation tests to verify the sales, payments, and payouts are all matching up correctly.

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
        print(f"Created cache directory: {CACHE_DIR}")

def select_payout_csv_file():
    """Allow user to select a CSV file from the payoutTransactionFiles folder"""
    
    # Check if payoutTransactionFiles folder exists
    if not os.path.exists(PAYOUT_FOLDER):
        print(f"ERROR: Folder '{PAYOUT_FOLDER}' not found!")
        print(f"Please create the folder and place your payout CSV files there")
        return None
    
    # Get list of CSV files in the folder
    csv_files = [f for f in os.listdir(PAYOUT_FOLDER) if f.lower().endswith('.csv')]
    
    if not csv_files:
        print(f"ERROR: No CSV files found in '{PAYOUT_FOLDER}' folder!")
        print(f"Please place your payout transaction CSV files in the '{PAYOUT_FOLDER}' folder")
        return None
    
    # Sort files by modification time (newest first) for convenience
    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(PAYOUT_FOLDER, x)), reverse=True)
    
    print(f"\nSELECT PAYOUT CSV FILE")
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
                    
                    print(f"\nSelected: {selected_file}")
                    print(f"Full path: {selected_path}")
                    
                    return selected_path
                else:
                    print(f"ERROR: Please enter a number between 1 and {len(csv_files)}")
            else:
                print("ERROR: Please enter a valid number")
                
        except KeyboardInterrupt:
            print("\nCancelled by user")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

def select_timezone_and_format():
    """Allow user to select timezone and format for reconciliation"""
    print(f"\nSELECT TIMEZONE FOR RECONCILIATION")
    print(f"=" * 45)
    print(f"Choose the timezone for grouping sales and payout data:\n")
    
    print(f"1. UTC (Recommended for payout reconciliation)")
    print(f"   • All data grouped by UTC dates")
    print(f"   • Payout transactions in original UTC timezone")
    print(f"   • Most accurate for matching Shopify payouts")
    print()
    
    print(f"2. Shop Timezone ({SALES_TIMEZONE})")
    print(f"   • All data grouped by {SALES_TIMEZONE} dates")
    print(f"   • Payout transactions converted to {SALES_TIMEZONE}")
    print(f"   • Better for business reporting and Shopify report comparison")
    print()
    
    # Get timezone selection
    while True:
        try:
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == "1":
                print(f"\nSelected: UTC timezone")
                use_utc_timezone = True
                target_timezone = "UTC"
                break
            elif choice == "2":
                print(f"\nSelected: {SALES_TIMEZONE} timezone")
                use_utc_timezone = False
                target_timezone = SALES_TIMEZONE
                break
            else:
                print("ERROR: Please enter 1 or 2")
                
        except KeyboardInterrupt:
            print("\nCancelled by user")
            return None, None, None
        except Exception as e:
            print(f"Error: {e}")
            return None, None, None
    
    # Get format selection
    print(f"\nSELECT OUTPUT FORMAT")
    print(f"=" * 30)
    print(f"Choose the CSV format:\n")
    
    print(f"1. Standard (Non-transposed)")
    print(f"   • Dates in rows, metrics in columns")
    print(f"   • Easier to work with in Excel")
    print(f"   • Good for data analysis and filtering")
    print()
    
    print(f"2. Transposed")
    print(f"   • Dates in columns, metrics in rows")
    print(f"   • Easier for bookkeepers to review")
    print(f"   • Better for monthly/daily comparisons")
    print()
    
    while True:
        try:
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == "1":
                print(f"\nSelected: Standard (Non-transposed) format")
                use_transposed = False
                break
            elif choice == "2":
                print(f"\nSelected: Transposed format")
                use_transposed = True
                break
            else:
                print("ERROR: Please enter 1 or 2")
                
        except KeyboardInterrupt:
            print("\nCancelled by user")
            return None, None, None
        except Exception as e:
            print(f"Error: {e}")
            return None, None, None
    
    return use_utc_timezone, target_timezone, use_transposed


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
        print(f"Cache expired ({cache_age_hours:.1f} hours old, max {CACHE_MAX_AGE_HOURS} hours)")
        return False
    
    print(f"Cache valid ({cache_age_hours:.1f} hours old)")
    return True

def load_from_cache(cache_file):
    """Load orders from cache file"""
    try:
        with open(cache_file, 'rb') as f:
            cached_data = pickle.load(f)
            print(f"Loaded {len(cached_data['orders'])} orders from cache")
            print(f"Cache created: {cached_data['created_at']}")
            return cached_data['orders']
    except Exception as e:
        print(f"Error loading cache: {e}")
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
        print(f"Saved {len(orders)} orders to cache")
        print(f"Cache file: {cache_file}")
    except Exception as e:
        print(f"Error saving cache: {e}")

def clear_cache():
    """Clear all cache files"""
    if os.path.exists(CACHE_DIR):
        import shutil
        shutil.rmtree(CACHE_DIR)
        print(f"Cleared cache directory: {CACHE_DIR}")

def show_cache_status():
    """Show current cache status and files"""
    print("\nCACHE STATUS")
    print("=" * 40)
    
    if not ENABLE_CACHE:
        print("Cache is disabled")
        return
    
    if not os.path.exists(CACHE_DIR):
        print("No cache directory found")
        return
    
    cache_files = [f for f in os.listdir(CACHE_DIR) if f.startswith('orders_cache_') and f.endswith('.pkl')]
    
    if not cache_files:
        print("Cache directory is empty")
        return
    
    print(f"Cache directory: {CACHE_DIR}")
    print(f"Found {len(cache_files)} cache files:")
    
    for cache_file in cache_files:
        cache_path = os.path.join(CACHE_DIR, cache_file)
        cache_age = time.time() - os.path.getmtime(cache_path)
        cache_age_hours = cache_age / 3600
        
        # Extract query hash from filename
        query_hash = cache_file.replace('orders_cache_', '').replace('.pkl', '')
        
        status = "VALID" if cache_age_hours <= CACHE_MAX_AGE_HOURS else "EXPIRED"
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
        print("\nCACHE MANAGEMENT")
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
                print("Cache clear cancelled")
        elif choice == "3":
            ENABLE_CACHE = not ENABLE_CACHE
            print(f"Cache is now {'ON' if ENABLE_CACHE else 'OFF'}")
        elif choice == "4":
            break
        else:
            print("Invalid choice")

# Load payout transactions CSV
def load_payout_csv(filepath):
    """
    Load and normalize payout transaction CSV data.
    IMPORTANT: The 'Amount' column in the payout CSV already includes both charges and refunds,
    with refunds already subtracted out. This means:
    - Positive amounts are net charges (charges minus any refunds for that payout date)
    - We don't need to separate charges and refunds and recombine them
    """
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
def extract_date_range(payout_df, use_utc_timezone):
    """Extract date range from payout data using the specified timezone"""
    if use_utc_timezone:
        # Use UTC dates directly
        min_date = payout_df['Transaction Date'].min().date()
        max_date = payout_df['Transaction Date'].max().date()
        print(f"Using UTC timezone for date range extraction")
    else:
        # Convert to shop timezone for date range
        payout_df_tz = payout_df.copy()
        payout_df_tz['Transaction Date'] = payout_df_tz['Transaction Date'].dt.tz_convert(SALES_TIMEZONE)
        min_date = payout_df_tz['Transaction Date'].min().date()
        max_date = payout_df_tz['Transaction Date'].max().date()
        print(f"Using {SALES_TIMEZONE} timezone for date range extraction")
    
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
                sourceName
                retailLocation {
                id
                name
                }
                createdAt
                processedAt
                displayFinancialStatus
                displayFulfillmentStatus
                totalPriceSet { presentmentMoney { amount currencyCode } }
                totalTaxSet { presentmentMoney { amount currencyCode } }
                totalShippingPriceSet { presentmentMoney { amount currencyCode } }
                totalRefundedSet { presentmentMoney { amount currencyCode } }
                subtotalPriceSet { shopMoney { amount currencyCode } }
                totalTipReceivedSet { presentmentMoney { amount currencyCode } }
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
                    refundLineItems(first: 10) {
                        nodes {
                        subtotalSet {
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        }
                    }
                    transactions(first: 10) {
                        nodes {
                        amountSet {
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        gateway
                        kind
                        status
                        processedAt
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
        
        print(f"Checking cache for date range {start_date} to {end_date}")
        print(f"Query hash: {query_hash}")
        
        if is_cache_valid(cache_file):
            cached_orders = load_from_cache(cache_file)
            if cached_orders is not None:
                print(f"Using cached data - skipping API calls!")
                return cached_orders
        
        print(f"Cache miss - fetching from Shopify API...")
    else:
        print(f"WARNING: Cache disabled - fetching fresh data from Shopify API...")
    
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
        
        print(f"Fetch #{fetch_count}: Querying orders {'from cursor ' + cursor[:20] + '...' if cursor else 'from start'}")
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

        print(f"   Found {batch_new_orders} new orders (total: {order_count})")
        
        if not data["data"]["orders"]["pageInfo"]["hasNextPage"]:
            print(f"Completed fetching {order_count} orders in {fetch_count} API calls")
            break

        cursor = data["data"]["orders"]["pageInfo"]["endCursor"]

    # Save to cache if enabled
    if ENABLE_CACHE:
        save_to_cache(cache_file, orders)

    return orders

from collections import defaultdict

def parse_orders(order_data, use_utc_timezone, target_timezone, payout_mapping=None):
    """Parse orders using payout-centric grouping approach"""
    # Create data structures for the selected timezone
    by_date = defaultdict(lambda: defaultdict(float))
    detailed_transactions = defaultdict(list)
    
    # Track order names and dates for each day
    order_info_by_date = defaultdict(list)
    
    # Track which orders have been processed to avoid double-counting order data
    processed_orders = set()
    
    print(f"Processing orders using {target_timezone} timezone")
    print(f"   PAYOUT-CENTRIC APPROACH: Grouping orders by payout dates")
    if payout_mapping:
        print(f"   Using payout-based grouping for {len(payout_mapping)} Shopify Payment transactions")
        print(f"   Supporting all payment methods: Shopify Payments (incl. manual), Cash, Gift Card, Shop Cash")
        print(f"   Credit card orders grouped by payout date, other payments by processedAt date")

    for order in order_data:
        # Calculate dates in the target timezone
        created_dt = pd.to_datetime(order['createdAt']).tz_convert(target_timezone)
        processed_dt = pd.to_datetime(order.get('processedAt', order['createdAt'])).tz_convert(target_timezone)
        
        created_date = created_dt.date()
        processed_date = processed_dt.date()
        
        # Basic order totals - all essential fields are guaranteed to exist
        # subtotalPriceSet is the gross sales (before discounts)
        # totalDiscountsSet is the discount amount (positive value)
        # Net sales = gross sales - discounts
        gross_sales = float(order['subtotalPriceSet']['presentmentMoney']['amount'])  # This is gross sales before discounts
        discounts = float(order['totalDiscountsSet']['presentmentMoney']['amount'])  # This is discount amount (positive)
        net_sales = gross_sales  # Net sales after discounts are subtracted
        gross_sales = net_sales + discounts  # Recalculate gross sales for clarity
        tax = float(order['totalTaxSet']['presentmentMoney']['amount'])
        shipping = float(order['totalShippingPriceSet']['presentmentMoney']['amount'])
        tips = float(order['totalTipReceivedSet']['presentmentMoney']['amount'])
        net_payment = float(order['netPaymentSet']['presentmentMoney']['amount'])
        order_refunds = float(order['totalRefundedSet']['presentmentMoney']['amount'])
        outstanding = float(order['totalOutstandingSet']['presentmentMoney']['amount'])
        
        # Calculate funds that should be collected (theoretical amount)
        funds_collected = net_sales + tax + shipping + tips

        # Store order data to be allocated to payout dates
        order_data_for_allocation = {
            'gross_sales': gross_sales,
            'net_sales': net_sales,
            'discounts': discounts,
            'tax': tax,
            'shipping': shipping,
            'tips': tips,
            'order_refunds': order_refunds,
            'net_payment': net_payment,
            'outstanding': outstanding,
            'funds_collected': funds_collected,  # This is the theoretical amount that should be collected
        }

        # PAYOUT-CENTRIC APPROACH: Process transactions to find payout dates
        # Process both Shopify Payments (credit card) and cash transactions
        order_name = order['name']
        order_has_credit_card_payment = False
        order_has_cash_payment = False
        order_payout_date = None
        
        # First pass: Find if this order has different payment types and determine payout date
        for txn in order['transactions']:
            if txn['status'] != "SUCCESS":
                continue
                
            gateway = txn['gateway']
            kind = txn['kind']
            
            # Check for Shopify Payments (credit card transactions)
            if gateway == 'shopify_payments' and kind in ['AUTHORIZATION', 'SALE']:
                order_has_credit_card_payment = True
                
                # Try to find payout date for this transaction
                if payout_mapping:
                    kind_mapping = {
                        'AUTHORIZATION': 'charge',
                        'SALE': 'charge',
                        'REFUND': 'refund',
                        'CAPTURE': 'charge'
                    }
                    
                    payout_type = kind_mapping.get(kind, kind.lower())
                    mapping_key = f"{order_name}_{payout_type}"
                    
                    if mapping_key in payout_mapping:
                        payout_info = payout_mapping[mapping_key]
                        order_payout_date = str(payout_info['payout_date'])
                        # print(f"   Found payout date {order_payout_date} for order {order_name}")
                        break
            
            # Check for cash transactions and other payment methods that use processedAt date
            elif gateway in ['cash', 'gift_card', 'shop_cash'] and kind in ['SALE']:
                order_has_cash_payment = True
                # For these payment methods, use the processedAt date as the payout date
                if not order_payout_date:  # Only set if we don't already have a payout date from credit card
                    cash_processed_date = pd.to_datetime(txn['processedAt']).tz_convert(target_timezone).date()
                    order_payout_date = str(cash_processed_date)
                    # print(f"   Found {gateway} payout date {order_payout_date} for order {order_name}")
        
        # Skip orders without recognized payment methods or without payout mapping
        if (not order_has_credit_card_payment and not order_has_cash_payment) or not order_payout_date:
            continue
        
        # Allocate order data to payout date (for orders with credit card or cash payments)
        if order_name not in processed_orders:
            processed_orders.add(order_name)
            
            # Allocate order data to the payout date
            for key, value in order_data_for_allocation.items():
                by_date[order_payout_date][key] += value
            
            by_date[order_payout_date]['order_count'] += 1
            
            # Track order info for the payout date
            order_info_by_date[order_payout_date].append({ 
                'name': order['name'],
                'created_at': created_dt.isoformat(),
                'processed_at': processed_dt.isoformat(),
                'created_datetime': created_dt,
                'original_created_date': str(created_date),
                'grouped_by_payout_date': order_payout_date
            })

        # Second pass: Process all transactions for this order
        for txn in order['transactions']:
            if txn['status'] != "SUCCESS":
                continue
                
            txn_date = pd.to_datetime(txn['processedAt']).tz_convert(target_timezone).date()
            txn_date_str = str(txn_date)
            
            amount = float(txn['amountSet']['presentmentMoney']['amount'])
            gateway = txn['gateway']
            kind = txn['kind']
            txn_id = txn['id']
            
            # For credit card transactions, use payout date
            if gateway == 'shopify_payments' and order_payout_date:
                final_date_str = order_payout_date
                use_payout_date = True
            # For cash and other payment methods, use processedAt date as payout date
            elif gateway in ['cash', 'gift_card', 'shop_cash']:
                final_date_str = str(txn_date)  # Use the transaction's processedAt date
                use_payout_date = False  # This is actually using processed date, not payout date
            else:
                # For other unrecognized payment methods, skip them
                continue
            
            # Track payment data by final date
            by_date[final_date_str][f'{gateway}_{kind.lower()}'] += amount
            
            # Categorize payments by gateway
            if gateway == 'shopify_payments':
                if kind == 'AUTHORIZATION':
                    by_date[final_date_str]['shopify_payments'] += amount
                elif kind == 'SALE':
                    by_date[final_date_str]['shopify_payments'] += amount
                elif kind == 'REFUND':
                    by_date[final_date_str]['shopify_payments_refunds'] -= amount
                # Note: CAPTURE is ignored to avoid double-counting with AUTHORIZATION
            elif gateway == 'cash':
                if kind == 'SALE':
                    by_date[final_date_str]['cash'] += amount
                elif kind == 'REFUND':
                    by_date[final_date_str]['cash_refunds'] -= amount
            elif gateway == 'manual':
                if kind == 'SALE':
                    by_date[final_date_str]['shopify_payments'] += amount
                elif kind == 'REFUND':
                    by_date[final_date_str]['shopify_payments_refunds'] -= amount
            elif gateway == 'gift_card':
                if kind == 'SALE':
                    by_date[final_date_str]['gift_card'] += amount
                elif kind == 'REFUND':
                    by_date[final_date_str]['gift_card_refunds'] -= amount
            elif gateway == 'shop_cash':
                if kind in ['AUTHORIZATION', 'SALE']:
                    by_date[final_date_str]['shop_cash'] += amount
                elif kind == 'REFUND':
                    by_date[final_date_str]['shop_cash_refunds'] -= amount
            
            # Store detailed transaction info
            detailed_transactions[final_date_str].append({
                'order_name': order['name'],
                'transaction_id': txn['id'],
                'kind': kind,
                'gateway': gateway,
                'amount': amount,
                'test': txn.get('test', False),
                'processed_date': str(txn_date),
                'grouped_by_date': final_date_str,
                'use_payout_date': use_payout_date,
                'timezone': target_timezone
            })

    return by_date, detailed_transactions, order_info_by_date

def write_outputs(by_date, detailed_transactions, order_info_by_date, payout_df, use_utc_timezone, target_timezone, use_transposed):
    """Write reconciliation outputs using the specified timezone and format"""
    
    # Generate reconciliation data
    print(f"   Generating {target_timezone} reconciliation data...")
    df = generate_reconciliation_dataframe(by_date, detailed_transactions, order_info_by_date, payout_df, target_timezone)
    
    # Generate filename with timezone
    timezone_suffix = "UTC" if use_utc_timezone else "ShopTimezone"
    
    if use_transposed:
        # Create transposed view and write transposed file
        print(f"   Creating transposed reconciliation view...")
        df_t = df.set_index("date").T
        filename = f"transposed_reconciliation_{timezone_suffix}_payout_grouped.csv"
        print(f"   Writing {filename}...")
        df_t.to_csv(filename)
        format_info = "transposed (dates in columns, metrics in rows)"
    else:
        # Write standard format
        filename = f"daily_sales_reconciliation_{timezone_suffix}_payout_grouped.csv"
        print(f"   Writing {filename}...")
        df.to_csv(filename, index=False)
        format_info = "standard (dates in rows, metrics in columns)"
    
    print(f"\nRECONCILIATION COMPLETE! File generated:")
    print(f"   Timezone: {target_timezone}")
    print(f"   Format: {format_info}")
    print(f"     • {filename}")
    print(f"   ")
    print(f"   PAYOUT-CENTRIC APPROACH:")
    print(f"   • Credit Card Orders (Shopify Payments & Manual): Grouped by payout date (matches bank deposits)")
    print(f"   • Cash Orders: Grouped by processedAt date (immediate payout)")
    print(f"   • Gift Card Orders: Grouped by processedAt date (immediate payout)")
    print(f"   • Shop Cash Orders: Grouped by processedAt date (immediate payout)")
    print(f"   • ALL PAYMENT METHODS: Now included in comprehensive reconciliation")
    print(f"   ")
    print(f"   TIMEZONE INFO:")
    if use_utc_timezone:
        print(f"   • All data grouped by UTC dates")
        print(f"   • Payout transactions in original UTC timezone")
        print(f"   • Recommended for accurate payout reconciliation")
    else:
        print(f"   • All data grouped by {SALES_TIMEZONE} dates")
        print(f"   • Payout transactions converted to {SALES_TIMEZONE}")
        print(f"   • Recommended for business reporting and Shopify report comparison")
    
    return df

def generate_reconciliation_dataframe(by_date, detailed_transactions, order_info_by_date, payout_df, timezone_name):
    """Generate reconciliation dataframe for a specific timezone"""
    df_rows = []
    
    # Convert payout dates to the specified timezone if not UTC
    if timezone_name != "UTC":
        print(f"   Converting payout transaction dates from UTC to {timezone_name} timezone...")
        payout_df_tz = payout_df.copy()
        # Convert Transaction Date to the specified timezone
        payout_df_tz['Transaction Date'] = payout_df_tz['Transaction Date'].dt.tz_convert(timezone_name)
        
        # Also convert Payout Date for completeness
        payout_df_tz['Payout Date'] = payout_df_tz['Payout Date'].dt.tz_convert(timezone_name)
        
        print(f"   Payout dates converted: Using {timezone_name} timezone for grouping")
    else:
        payout_df_tz = payout_df  # Use original UTC dates
    
    # Get all unique dates from both orders and payouts
    all_dates = set(by_date.keys())
    payout_dates = set(payout_df_tz['Payout Date'].dt.date.astype(str))
    all_dates.update(payout_dates)
    
    for date_str in sorted(all_dates):
        metrics = by_date.get(date_str, defaultdict(float))
        orders_info = order_info_by_date.get(date_str, [])
        
        row = {"date": date_str, "timezone": timezone_name}
        
        # Order tracking info - earliest and latest orders
        if orders_info:
            sorted_orders = sorted(orders_info, key=lambda x: x['created_datetime'])
            earliest_order = sorted_orders[0]
            latest_order = sorted_orders[-1]
            
            row.update({
                'earliest_order_name': earliest_order['name'],
                'earliest_order_created_at': earliest_order['created_at'],
                'latest_order_name': latest_order['name'],
                'latest_order_created_at': latest_order['created_at'],
                'order_count': len(orders_info),
            })
        else:
            row.update({
                'earliest_order_name': '',
                'earliest_order_created_at': '',
                'latest_order_name': '',
                'latest_order_created_at': '',
                'order_count': 0,
            })
        
        # === SALES SECTION ===
        gross_sales = metrics.get('gross_sales', 0)  # This is from subtotalPriceSet (gross sales before discounts)
        discounts = metrics.get('discounts', 0)  # This is discount amount (positive value)
        net_sales = metrics.get('net_sales', 0)  # This is gross_sales - discounts (net sales after discounts)
        tax = metrics.get('tax', 0)
        shipping = metrics.get('shipping', 0)
        tips = metrics.get('tips', 0)
        funds_collected = metrics.get('funds_collected', 0)
        
        # Overall sales totals
        row.update({
            'sales_gross_sales': gross_sales,
            'sales_discounts': -discounts,  # Display discounts as negative numbers for reporting
            'sales_net_sales': net_sales,
            'sales_tax': tax,
            'sales_shipping': shipping,
            'sales_tips': tips,
            'sales_funds_collected': funds_collected,  # Theoretical amount (net + tax + shipping + tips)
            'sales_order_count': metrics.get('order_count', 0),
        })
        
        # === PAYMENTS SECTION ===
        # Payment gateway breakdown with exact matching
        row.update({
            'payments_shopify_payments': metrics.get('shopify_payments', 0),  # Includes manual payments
            'payments_cash': metrics.get('cash', 0),
            'payments_gift_card': metrics.get('gift_card', 0),
            'payments_shop_cash': metrics.get('shop_cash', 0),
            'payments_other': metrics.get('other_payments', 0),
        })
        
        # Refunds by payment type
        row.update({
            'payments_shopify_refunds': metrics.get('shopify_payments_refunds', 0),  # Includes manual refunds
            'payments_cash_refunds': metrics.get('cash_refunds', 0),
            'payments_gift_card_refunds': metrics.get('gift_card_refunds', 0),
            'payments_shop_cash_refunds': metrics.get('shop_cash_refunds', 0),
            'payments_other_refunds': metrics.get('other_refunds', 0),
        })
        
        # Special cash handling
        row.update({
            'payments_cash_change': metrics.get('cash_change', 0),  # Cash change given
        })
        
        # Total refunds from orders (this is the aggregate from order totals)
        row.update({
            'payments_total_refunds': metrics.get('order_refunds', 0),
        })
        
        # === SHOPIFY PAYOUTS SECTION ===
        # Payout data reconciliation using timezone-converted dates
        date_obj = pd.to_datetime(date_str).date()
        payout_day = payout_df_tz[payout_df_tz['Payout Date'].dt.date == date_obj]
        
        # Filter out pending payouts (only include 'paid' transactions)
        payout_day_paid = payout_day[payout_day['Payout Status'] == 'paid']
        
        # Separate payout data by type (only for paid transactions)
        # NOTE: Amount column already has refunds netted out, so we don't need to separate and recombine
        payout_refunds = payout_day_paid[payout_day_paid['Type'] == 'refund']
        payout_charges = payout_day_paid[payout_day_paid['Type'] == 'charge']
        payout_adjustments = payout_day_paid[payout_day_paid['Type'] == 'adjustment']
        payout_chargebacks = payout_day_paid[payout_day_paid['Type'] == 'chargeback']
        payout_chargebacks_won = payout_day_paid[payout_day_paid['Type'] == 'chargeback won']
        payout_shop_cash_credit = payout_day_paid[payout_day_paid['Type'] == 'shop_cash_credit']    
        payout_other = payout_day_paid[~payout_day_paid['Type'].isin(['charge', 'refund', 'adjustment', 'chargeback', 'chargeback won', 'shop_cash_credit'])]
        
        # Calculate Shopify payout metrics
        # Since Amount already has refunds netted out, use total Amount for all charge+refund transactions
        charge_and_refund_transactions = payout_day_paid[payout_day_paid['Type'].isin(['charge', 'refund'])]
        shopify_net_payments = charge_and_refund_transactions['Payout Amount'].sum()  # This is already net of refunds
        shopify_payout_refunds = abs(payout_refunds['Payout Amount'].sum())  # Track refunds separately for reporting
        shopify_gross_charges = payout_charges['Payout Amount'].sum()  # Gross charges before refunds
        shopify_fees = payout_day_paid['Payout Fee'].sum()  # Use 'Payout Fee' column
        shopify_net_deposit = payout_day_paid['Payout Net Deposit'].sum()  # Use 'Payout Net Deposit' column
        
        row.update({
            'shopify_payments_amount': shopify_net_payments,  # Net payments (already includes refunds)
            'shopify_payout_refunds': shopify_payout_refunds,  # Refunds for reporting
            'shopify_gross_charges': shopify_gross_charges,  # Gross charges before refunds
            'shopify_fees': -shopify_fees,  # Negative for reporting (cost)
            'shopify_net_deposit': shopify_net_deposit,
        })
        
        # Additional metrics for detailed analysis (optional, can be removed later)
        row.update({
            'payout_count': len(payout_day_paid),
            'payout_statuses': ", ".join(sorted(payout_day_paid['Payout Status'].unique())) if len(payout_day_paid) > 0 else "",
            'payout_types': ", ".join(sorted(payout_day_paid['Type'].unique())) if len(payout_day_paid) > 0 else "",
            'payout_type_adjustment': payout_adjustments['Payout Amount'].sum(),
            'payout_type_chargeback': payout_chargebacks['Payout Amount'].sum(),
            'payout_type_chargeback_won': payout_chargebacks_won['Payout Amount'].sum(),
            'payout_type_shop_cash_credit': payout_shop_cash_credit['Payout Amount'].sum(),
            'payout_type_other': payout_other['Payout Amount'].sum(),
            # Track pending transactions separately for visibility
            'pending_payout_amount': payout_day[payout_day['Payout Status'] != 'paid']['Payout Amount'].sum(),
            'pending_payout_count': len(payout_day[payout_day['Payout Status'] != 'paid']),
        })

        # === RECONCILIATION ANALYSIS ===
        # Use the corrected metric names for reconciliation calculations
        # CRITICAL FIX: Order side must be NET payments (gross charges minus refunds)
        # to match the payout side which already has refunds netted out
        order_side_gross_payments = row['payments_shopify_payments']  # Gross Shopify payments from order transactions (includes manual)
        order_side_refunds = row['payments_shopify_refunds']  # Refunds from order transactions (includes manual)
        order_side_net_payments = order_side_gross_payments - order_side_refunds  # Net payments after refunds
        
        payout_side_net_payments = row['shopify_payments_amount']  # Net payments from payout CSV (Amount column, already net of refunds)
        shopify_fees = row['shopify_fees']  # Fees from payout data
        shopify_net_deposit = row['shopify_net_deposit']  # Net deposit from payout data
        
        # CORRECTED RECONCILIATION LOGIC:
        # The payout CSV Amount column already has refunds netted out, so we compare:
        # - Order side: NET Shopify + Manual payments (gross charges minus refunds)
        # - Payout side: NET payment amount from CSV (Amount column, which includes both charges and refunds)
        
        # Check if amounts match (within 1 cent tolerance) - only for days with paid payouts
        mismatch = abs(order_side_net_payments - payout_side_net_payments) > 0.01 if payout_side_net_payments != 0 else False
        
        # === ADDITIONAL RECONCILIATION CHECKS ===
        
        # 1. Sales vs Total Payments Reconciliation
        # Calculate total payments received across all payment methods (gross)
        total_payments_gross = (row['payments_shopify_payments'] + row['payments_cash'] + 
                               row['payments_gift_card'] + row['payments_shop_cash'])
        
        # Calculate total refunds across all payment methods
        total_refunds_all = (row['payments_shopify_refunds'] + row['payments_cash_refunds'] + 
                            row['payments_gift_card_refunds'] + row['payments_shop_cash_refunds'])
        
        # Calculate net payments across all methods
        total_payments_net = total_payments_gross - total_refunds_all
        
        # Expected amount to collect (sales + tax + shipping + tips)
        expected_funds_collected = row['sales_funds_collected']
        
        # Check if sales funds match total payments received
        sales_vs_payments_diff = expected_funds_collected - total_payments_net
        sales_vs_payments_mismatch = abs(sales_vs_payments_diff) > 0.01 if expected_funds_collected != 0 else False
        
        row.update({
            'reconciliation_difference': order_side_net_payments - payout_side_net_payments,
            'reconciliation_mismatch': mismatch,
            'reconciliation_order_side_net': order_side_net_payments,  # Order side net payments
            'reconciliation_payout_side_net': payout_side_net_payments,  # Payout side net payments
            'reconciliation_timezone_note': f"Payout dates converted to {timezone_name}" if timezone_name != "UTC" else "Original UTC dates",
            
            # Sales vs Payments reconciliation
            'sales_vs_payments_expected': expected_funds_collected,  # What we should collect
            'sales_vs_payments_actual_gross': total_payments_gross,  # What we actually received (gross)
            'sales_vs_payments_actual_net': total_payments_net,  # What we actually received (net)
            'sales_vs_payments_difference': sales_vs_payments_diff,  # Difference
            'sales_vs_payments_mismatch': sales_vs_payments_mismatch,  # True if mismatch
        })
        
        df_rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(df_rows).sort_values(by="date")
    return df

def create_order_payout_mapping(payout_df, target_timezone):
    """Create mapping from order names to payout dates for Shopify Payments"""
    order_payout_mapping = {}
    
    # Convert payout dates to target timezone if needed
    if target_timezone != "UTC":
        payout_df_tz = payout_df.copy()
        payout_df_tz['Payout Date'] = payout_df_tz['Payout Date'].dt.tz_convert(target_timezone)
        payout_df_tz['Transaction Date'] = payout_df_tz['Transaction Date'].dt.tz_convert(target_timezone)
    else:
        payout_df_tz = payout_df
    
    # Show available columns for debugging
    print(f"   Available payout CSV columns: {list(payout_df_tz.columns)}")
    
    # Create mapping for all transaction types that should be grouped by payout date
    shopify_payment_types = ['charge', 'refund', 'adjustment', 'chargeback', 'chargeback won']
    
    for _, row in payout_df_tz.iterrows():
        order_name = row.get('Order', '')
        payout_date = row['Payout Date'].date()
        transaction_type = row.get('Type', '')
        
        # Only map Shopify Payment transactions to payout dates
        if transaction_type in shopify_payment_types and order_name:
            # Create a unique key for each order-type combination
            mapping_key = f"{order_name}_{transaction_type}"
            
            if mapping_key not in order_payout_mapping:
                order_payout_mapping[mapping_key] = {
                    'order_name': order_name,
                    'payout_date': payout_date,
                    'transaction_type': transaction_type,
                    'payout_status': row.get('Payout Status', 'unknown'),
                    'amount': row.get('Amount', 0.0),
                    'fee': row.get('Fee', 0.0),
                    'net': row.get('Net', 0.0)
                }
    
    print(f"   Created order-to-payout mapping for {len(order_payout_mapping)} Shopify Payment entries")
    return order_payout_mapping

# Main execution
def main():
    global ENABLE_CACHE, CACHE_MAX_AGE_HOURS, SALES_TIMEZONE, CACHE_DIR
  
    print("SHOPIFY PAYMENT RECONCILIATION")
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
            print("WARNING: Cache disabled for this run")
    
    # Show cache status
    if ENABLE_CACHE:
        print(f"Cache: ENABLED (max age: {CACHE_MAX_AGE_HOURS} hours)")
        if os.path.exists(CACHE_DIR):
            cache_files = [f for f in os.listdir(CACHE_DIR) if f.startswith('orders_cache_')]
            if cache_files:
                print(f"   {len(cache_files)} cache files found")
        print("   Use --cache for management, --clear-cache to clear, --no-cache to disable")
    else:
        print("Cache: DISABLED")
    print()
    
    # Select timezone
    use_utc_timezone, target_timezone, use_transposed = select_timezone_and_format()
    if use_utc_timezone is None:
        print("No timezone selected. Exiting.")
        sys.exit(1)
    
    # Select payout CSV file
    selected_payout_file = select_payout_csv_file()
    if selected_payout_file is None:
        print("No payout file selected. Exiting.")
        sys.exit(1)
    
    print("\nLoading payout CSV file...")
    payout_df = load_payout_csv(selected_payout_file)
    print(f"   Loaded {len(payout_df)} payout transactions")
    
    start_date, end_date = extract_date_range(payout_df, use_utc_timezone)
    print(f"Reconciliation period: {start_date} to {end_date}")

    print("\nFetching orders from Shopify GraphQL API...")
    order_data = fetch_orders(start_date, end_date)
    print(f"   Retrieved {len(order_data)} orders total")

    print("\nCreating transaction-to-payout mapping...")
    payout_mapping = create_order_payout_mapping(payout_df, target_timezone)

    print("\nProcessing orders and calculating daily summaries...")
    by_date, detailed_transactions, order_info_by_date = parse_orders(order_data, use_utc_timezone, target_timezone, payout_mapping)
    
    print(f"  Processed orders for {target_timezone} timezone")
    
    print("\nWriting output files and reconciling with payouts...")
    df = write_outputs(by_date, detailed_transactions, order_info_by_date, payout_df, use_utc_timezone, target_timezone, use_transposed)
    
    # print(f"\nDaily Summary Preview:")
    # print(df.head(10).to_string(index=False))
    
    # Show summary statistics
    total_sales = df['sales_net_sales'].sum()
    print(f"\nSummary Statistics:")
    print(f"Total net sales ({target_timezone} grouping): ${total_sales:,.2f}")
    print(f"Total orders: {df['order_count'].sum()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Count mismatches
    mismatches = (df['reconciliation_mismatch'] == True).sum()  # Count True values
    sales_vs_payments_mismatches = (df['sales_vs_payments_mismatch'] == True).sum()  # Count sales vs payments mismatches
    print(f"Shopify Payments vs Payout mismatches: {mismatches}")
    print(f"Sales vs Total Payments mismatches: {sales_vs_payments_mismatches}")
    
    print("Reconciliation complete.")

if __name__ == "__main__":
    main()

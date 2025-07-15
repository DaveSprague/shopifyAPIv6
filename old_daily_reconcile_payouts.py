# daily_reconcile_payouts.py
# Single-day reconciliation with transaction-level detail

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
from collections import defaultdict
import pytz

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

def select_date_and_timezone():
    """Allow user to select a specific date and timezone for analysis"""
    print(f"\n📅 SELECT DATE FOR ANALYSIS")
    print(f"=" * 35)
    
    # Get date from user
    while True:
        try:
            date_input = input("Enter date (YYYY-MM-DD): ").strip()
            # Validate date format
            target_date = datetime.strptime(date_input, '%Y-%m-%d').date()
            print(f"✅ Selected date: {target_date}")
            break
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD")
        except KeyboardInterrupt:
            print("\n🚪 Cancelled by user")
            return None, None, None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None, None, None
    
    print(f"\n🌍 SELECT TIMEZONE FOR ANALYSIS")
    print(f"=" * 35)
    print(f"Choose the timezone for grouping transactions:\n")
    
    print(f"1. UTC")
    print(f"   • All data grouped by UTC dates")
    print(f"   • Payout transactions in original UTC timezone")
    print(f"   • Most accurate for matching Shopify payouts")
    print()
    
    print(f"2. Shop Timezone ({SALES_TIMEZONE})")
    print(f"   • All data grouped by {SALES_TIMEZONE} dates")
    print(f"   • Payout transactions converted to {SALES_TIMEZONE}")
    print(f"   • Better for business reporting")
    print()
    
    # Get timezone selection
    while True:
        try:
            choice = input("Enter your choice (1-2): ").strip()
            
            if choice == "1":
                print(f"\n✅ Selected: UTC timezone")
                use_utc_timezone = True
                target_timezone = "UTC"
                break
            elif choice == "2":
                print(f"\n✅ Selected: {SALES_TIMEZONE} timezone")
                use_utc_timezone = False
                target_timezone = SALES_TIMEZONE
                break
            else:
                print("❌ Please enter 1 or 2")
                
        except KeyboardInterrupt:
            print("\n🚪 Cancelled by user")
            return None, None, None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None, None, None
    
    return target_date, use_utc_timezone, target_timezone

def load_payout_csv(file_path):
    """Load payout CSV file and return DataFrame"""
    try:
        df = pd.read_csv(file_path)
        print(f"📊 CSV columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ Error loading CSV file: {e}")
        return None

def get_cache_key(start_date, end_date):
    """Generate cache key for given date range"""
    date_str = f"{start_date}_{end_date}"
    return hashlib.md5(date_str.encode()).hexdigest()

def is_cache_valid(cache_file):
    """Check if cache file exists and is not expired"""
    if not os.path.exists(cache_file):
        return False
    
    # Check if cache is within max age
    cache_age = time.time() - os.path.getmtime(cache_file)
    max_age_seconds = CACHE_MAX_AGE_HOURS * 3600
    
    return cache_age < max_age_seconds

def save_to_cache(data, cache_file):
    """Save data to cache file"""
    try:
        create_cache_dir()
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
        print(f"💾 Data cached to: {cache_file}")
    except Exception as e:
        print(f"⚠️  Failed to save cache: {e}")

def load_from_cache(cache_file):
    """Load data from cache file"""
    try:
        with open(cache_file, 'rb') as f:
            data = pickle.load(f)
        print(f"📦 Data loaded from cache: {cache_file}")
        return data
    except Exception as e:
        print(f"⚠️  Failed to load cache: {e}")
        return None

def fetch_orders(start_date, end_date):
    """Fetch orders from Shopify GraphQL API with caching"""
    
    # Check cache first
    cache_key = get_cache_key(start_date, end_date)
    cache_file = os.path.join(CACHE_DIR, f"orders_cache_{cache_key}.pkl")
    
    if ENABLE_CACHE and is_cache_valid(cache_file):
        cached_data = load_from_cache(cache_file)
        if cached_data:
            return cached_data
    
    orders = []
    cursor = None
    page = 1
    
    # Expand date range by 1 day on each side to ensure we capture all transactions
    extended_start = (start_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    extended_end = (end_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    while True:
        after_clause = f', after: "{cursor}"' if cursor else ""
        
        query = f"""
        {{
          orders(first: 250, query: "created_at:>={extended_start} AND created_at:<={extended_end}"{after_clause}) {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
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
                
                transactions {{
                  id
                  kind
                  gateway
                  status
                  processedAt
                  test
                  amountSet {{
                    presentmentMoney {{
                      amount
                    }}
                  }}
                }}
                refunds {{
                  id
                  createdAt
                  totalRefundedSet {{
                    presentmentMoney {{
                      amount
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        response = requests.post(SHOPIFY_URL, json={"query": query}, headers=HEADERS)
        
        if response.status_code != 200:
            print(f"❌ Error fetching orders: {response.status_code}")
            print(response.text)
            break
        
        data = response.json()
        
        if "errors" in data:
            print(f"❌ GraphQL errors: {data['errors']}")
            break
        
        orders_data = data["data"]["orders"]
        orders.extend([edge["node"] for edge in orders_data["edges"]])
        
        print(f"📦 Fetched page {page}: {len(orders_data['edges'])} orders (total: {len(orders)})")
        
        if not orders_data["pageInfo"]["hasNextPage"]:
            break
        
        cursor = orders_data["pageInfo"]["endCursor"]
        page += 1
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)
    
    # Save to cache
    if ENABLE_CACHE:
        save_to_cache(orders, cache_file)
    
    return orders

def parse_orders_for_date(order_data, target_date, use_utc_timezone, target_timezone, payout_df):
    """Parse orders and return order-level details for a specific date with payout data"""
    
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    # Convert timezone abbreviations to full names
    if target_timezone == "UTC":
        target_tz = timezone.utc
    elif target_timezone == "US/Eastern":
        target_tz = pytz.timezone('US/Eastern')
    else:
        target_tz = pytz.timezone(target_timezone)
    
    # Process payout data for the target date
    payout_df_copy = payout_df.copy()
    
    # Determine which date column to use for payouts
    date_column = None
    for col in ['Payout Date', 'Date', 'Transaction Date', 'Available On']:
        if col in payout_df_copy.columns:
            date_column = col
            break
    
    if date_column:
        # Convert payout dates to target timezone
        if use_utc_timezone:
            payout_df_copy['payout_date_tz'] = pd.to_datetime(payout_df_copy[date_column]).dt.strftime('%Y-%m-%d')
        else:
            try:
                utc = pytz.UTC
                shop_tz = pytz.timezone(target_timezone)
                payout_dates = pd.to_datetime(payout_df_copy[date_column])
                payout_dates_utc = payout_dates.dt.tz_localize(utc)
                payout_dates_shop = payout_dates_utc.dt.tz_convert(shop_tz)
                payout_df_copy['payout_date_tz'] = payout_dates_shop.dt.strftime('%Y-%m-%d')
            except:
                payout_df_copy['payout_date_tz'] = pd.to_datetime(payout_df_copy[date_column]).dt.strftime('%Y-%m-%d')
        
        # Filter payout data for target date
        payout_day = payout_df_copy[payout_df_copy['payout_date_tz'] == target_date_str]
    else:
        payout_day = pd.DataFrame()
    
    order_rows = []
    
    for order in order_data:
        order_created = datetime.fromisoformat(order['createdAt'].replace('Z', '+00:00'))
        
        # Convert to target timezone
        if use_utc_timezone:
            order_created_tz = order_created.astimezone(timezone.utc)
        else:
            order_created_tz = order_created.astimezone(target_tz)
        
        order_date_str = order_created_tz.strftime('%Y-%m-%d')
        
        # Only process orders from the target date
        if order_date_str != target_date_str:
            continue
        
        # Extract order financial data
        gross_sales = float(order['subtotalPriceSet']['presentmentMoney']['amount'])
        net_sales = float(order['totalPriceSet']['presentmentMoney']['amount'])
        discounts = float(order['totalDiscountsSet']['presentmentMoney']['amount'])
        tax = float(order['totalTaxSet']['presentmentMoney']['amount'])
        shipping = float(order['totalShippingPriceSet']['presentmentMoney']['amount'])
        tips = float(order['totalTipsReceivedSet']['presentmentMoney']['amount'])
        order_refunds = float(order['totalRefundedSet']['presentmentMoney']['amount'])
        total_received = float(order['totalReceivedSet']['presentmentMoney']['amount'])
        outstanding = float(order['totalOutstandingSet']['presentmentMoney']['amount'])
        
        # Calculate funds collected
        funds_collected = net_sales + tax + shipping + tips
        
        # Initialize payment tracking
        payment_totals = {
            'shopify_payments': 0.0,
            'cash': 0.0,
            'manual': 0.0,
            'gift_card': 0.0,
            'shop_cash': 0.0,
            'other_payments': 0.0,
            'shopify_payments_refunds': 0.0,
            'cash_refunds': 0.0,
            'manual_refunds': 0.0,
            'gift_card_refunds': 0.0,
            'shop_cash_refunds': 0.0,
            'other_refunds': 0.0,
            'cash_change': 0.0
        }
        
        # Process transactions for this order
        for txn in order['transactions']:
            if txn['status'] != 'SUCCESS':
                continue
            
            amount = float(txn['amountSet']['presentmentMoney']['amount'])
            gateway = txn['gateway']
            kind = txn['kind']
            
            # Categorize payments using the same logic as the main script
            if gateway == 'shopify_payments':
                if kind == 'AUTHORIZATION':
                    payment_totals['shopify_payments'] += amount
                elif kind == 'SALE':
                    payment_totals['shopify_payments'] += amount
                elif kind == 'REFUND':
                    payment_totals['shopify_payments_refunds'] += amount
            
            elif gateway == 'cash':
                if kind == 'SALE':
                    payment_totals['cash'] += amount
                elif kind == 'REFUND':
                    payment_totals['cash_refunds'] += amount
                elif kind == 'CHANGE':
                    payment_totals['cash_change'] += amount
            
            elif gateway == 'manual':
                if kind == 'SALE':
                    payment_totals['manual'] += amount
                elif kind == 'REFUND':
                    payment_totals['manual_refunds'] += amount
            
            elif gateway == 'gift_card':
                if kind == 'SALE':
                    payment_totals['gift_card'] += amount
                elif kind == 'REFUND':
                    payment_totals['gift_card_refunds'] += amount
            
            elif gateway == 'shop_cash':
                if kind == 'AUTHORIZATION':
                    payment_totals['shop_cash'] += amount
                elif kind == 'SALE':
                    payment_totals['shop_cash'] += amount
                elif kind == 'REFUND':
                    payment_totals['shop_cash_refunds'] += amount
            
            else:
                if kind in ['SALE', 'AUTHORIZATION']:
                    payment_totals['other_payments'] += amount
                elif kind == 'REFUND':
                    payment_totals['other_refunds'] += amount
        
        # Process refunds data from GraphQL
        refunds_info = []
        for refund in order.get('refunds', []):
            refund_amount = float(refund['totalRefundedSet']['presentmentMoney']['amount'])
            refund_created = refund['createdAt']
            refunds_info.append({
                'id': refund['id'],
                'amount': refund_amount,
                'created_at': refund_created
            })
        
        # Create refunds summary string
        refunds_summary = ""
        if refunds_info:
            refunds_summary = f"{len(refunds_info)} refunds: " + ", ".join([f"${r['amount']:.2f}" for r in refunds_info])
        
        # Find payout data for this order
        order_payouts = payout_day[payout_day['Order'] == order['name']] if len(payout_day) > 0 else pd.DataFrame()
        
        # Calculate payout metrics
        if len(order_payouts) > 0:
            payout_paid = order_payouts[order_payouts['Payout Status'] == 'paid'] if 'Payout Status' in order_payouts.columns else order_payouts
            payout_charges = payout_paid[payout_paid['Type'] == 'charge'] if 'Type' in payout_paid.columns else payout_paid
            payout_refunds = payout_paid[payout_paid['Type'] == 'refund'] if 'Type' in payout_paid.columns else pd.DataFrame()
            
            shopify_payout_refunds = abs(payout_refunds['Amount'].sum()) if len(payout_refunds) > 0 else 0.0
            shopify_amount_before_fees = payout_charges['Amount'].sum() if len(payout_charges) > 0 else 0.0
            shopify_fees = payout_paid['Fee'].sum() if 'Fee' in payout_paid.columns else 0.0
            shopify_net_deposit = (payout_paid['Amount'].sum() - shopify_fees) if len(payout_paid) > 0 else 0.0
            payout_count = len(payout_paid)
            payout_statuses = ", ".join(sorted(payout_paid['Payout Status'].unique())) if 'Payout Status' in payout_paid.columns else ""
            payout_types = ", ".join(sorted(payout_paid['Type'].unique())) if 'Type' in payout_paid.columns else ""
        else:
            shopify_payout_refunds = 0.0
            shopify_amount_before_fees = 0.0
            shopify_fees = 0.0
            shopify_net_deposit = 0.0
            payout_count = 0
            payout_statuses = ""
            payout_types = ""
        
        # Calculate reconciliation metrics
        total_shopify_receipts = payment_totals['shopify_payments'] + shopify_payout_refunds
        shopify_payout_w_refunds_before_fees = shopify_amount_before_fees + shopify_payout_refunds
        reconciliation_difference = total_shopify_receipts - shopify_payout_w_refunds_before_fees
        reconciliation_mismatch = abs(reconciliation_difference) > 0.01 if shopify_payout_w_refunds_before_fees > 0 else False
        reconciliation_percentage = (shopify_payout_w_refunds_before_fees / total_shopify_receipts * 100) if total_shopify_receipts != 0 else 0
        
        # Create order row with all the same columns as the main script
        order_row = {
            'date': order_date_str,
            'timezone': target_timezone,
            'order_name': order['name'],
            'order_id': order['id'],
            'order_created_at': order_created_tz.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'order_count': 1,
            
            # Sales section
            'sales_gross_sales': gross_sales,
            'sales_discounts': -abs(discounts),
            'sales_net_sales': net_sales,
            'sales_tax': tax,
            'sales_shipping': shipping,
            'sales_tips': tips,
            'sales_total_received': total_received,
            'sales_funds_collected': funds_collected,
            
            # Payments section
            'payments_shopify_payments': payment_totals['shopify_payments'],
            'payments_cash': payment_totals['cash'],
            'payments_manual': payment_totals['manual'],
            'payments_gift_card': payment_totals['gift_card'],
            'payments_shop_cash': payment_totals['shop_cash'],
            'payments_other': payment_totals['other_payments'],
            
            # Refunds section
            'payments_shopify_refunds': payment_totals['shopify_payments_refunds'],
            'payments_cash_refunds': payment_totals['cash_refunds'],
            'payments_manual_refunds': payment_totals['manual_refunds'],
            'payments_gift_card_refunds': payment_totals['gift_card_refunds'],
            'payments_shop_cash_refunds': payment_totals['shop_cash_refunds'],
            'payments_other_refunds': payment_totals['other_refunds'],
            'payments_cash_change': payment_totals['cash_change'],
            'payments_total_refunds': order_refunds,
            
            # Shopify payouts section
            'shopify_payout_refunds': shopify_payout_refunds,
            'shopify_amount_before_fees': shopify_amount_before_fees,
            'shopify_fees': shopify_fees,
            'shopify_net_deposit': shopify_net_deposit,
            
            # Payout details
            'payout_count': payout_count,
            'payout_statuses': payout_statuses,
            'payout_types': payout_types,
            'pending_payout_amount': 0.0,  # Will be calculated if needed
            'pending_payout_count': 0,
            
            # Refunds details
            'refunds_summary': refunds_summary,
            'refunds_count': len(refunds_info),
            
            # Reconciliation section
            'reconciliation_difference': reconciliation_difference,
            'reconciliation_mismatch': reconciliation_mismatch,
            'reconciliation_percentage': reconciliation_percentage,
            'reconciliation_total_receipts': total_shopify_receipts,
            'reconciliation_expected_payout': shopify_payout_w_refunds_before_fees,
            'reconciliation_timezone_note': f"Payout dates converted to {target_timezone}" if target_timezone != "UTC" else "Original UTC dates"
        }
        
        order_rows.append(order_row)
    
    return order_rows

def filter_payout_transactions(payout_df, target_date, use_utc_timezone):
    """Filter payout transactions for the target date - kept for compatibility"""
    
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    # Determine which date column to use
    date_column = None
    for col in ['Payout Date', 'Date', 'Transaction Date', 'Available On']:
        if col in payout_df.columns:
            date_column = col
            break
    
    if date_column is None:
        print(f"❌ No recognized date column found in CSV. Available columns: {list(payout_df.columns)}")
        return pd.DataFrame()
    
    print(f"📅 Using date column: '{date_column}'")
    
    # Convert payout date to target timezone
    if use_utc_timezone:
        # Assume payout dates are in UTC and just extract the date part
        payout_df['payout_date_tz'] = pd.to_datetime(payout_df[date_column]).dt.strftime('%Y-%m-%d')
    else:
        # For shop timezone, handle timezone conversion
        try:
            utc = pytz.UTC
            shop_tz = pytz.timezone(SALES_TIMEZONE)
            
            # Parse dates, assume UTC, then convert to shop timezone
            payout_dates = pd.to_datetime(payout_df[date_column])
            payout_dates_utc = payout_dates.dt.tz_localize(utc)
            payout_dates_shop = payout_dates_utc.dt.tz_convert(shop_tz)
            payout_df['payout_date_tz'] = payout_dates_shop.dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"⚠️  Timezone conversion error: {e}")
            print("🔄 Falling back to simple date parsing...")
            # Fallback to simple date parsing if timezone conversion fails
            payout_df['payout_date_tz'] = pd.to_datetime(payout_df[date_column]).dt.strftime('%Y-%m-%d')
    
    # Filter for target date
    filtered_df = payout_df[payout_df['payout_date_tz'] == target_date_str].copy()
    
    print(f"📊 Found {len(filtered_df)} payout transactions for {target_date_str}")
    
    return filtered_df

def main():
    print("🚀 DAILY SHOPIFY PAYMENT RECONCILIATION")
    print("=" * 60)
    
    # Select date and timezone
    target_date, use_utc_timezone, target_timezone = select_date_and_timezone()
    if target_date is None:
        print("❌ No date selected. Exiting.")
        sys.exit(1)
    
    # Select payout CSV file
    selected_payout_file = select_payout_csv_file()
    if selected_payout_file is None:
        print("❌ No payout file selected. Exiting.")
        sys.exit(1)
    
    print("\n📂 Loading payout CSV file...")
    payout_df = load_payout_csv(selected_payout_file)
    if payout_df is None:
        print("❌ Failed to load payout file. Exiting.")
        sys.exit(1)
    
    print(f"   ✅ Loaded {len(payout_df)} payout transactions")
    
    # Set date range for fetching orders (target date +/- 1 day for safety)
    start_date = pd.to_datetime(target_date) - pd.Timedelta(days=1)
    end_date = pd.to_datetime(target_date) + pd.Timedelta(days=1)
    
    print(f"\n🛒 Fetching orders from Shopify GraphQL API...")
    print(f"   📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    order_data = fetch_orders(start_date, end_date)
    print(f"   ✅ Retrieved {len(order_data)} orders total")
    
    print(f"\n🔄 Processing orders for {target_date.strftime('%Y-%m-%d')}...")
    order_rows = parse_orders_for_date(order_data, target_date, use_utc_timezone, target_timezone, payout_df)
    print(f"   ✅ Found {len(order_rows)} orders for {target_date.strftime('%Y-%m-%d')}")
    
    if len(order_rows) == 0:
        print("❌ No orders found for the selected date.")
        return
    
    # Create DataFrame
    df = pd.DataFrame(order_rows)
    
    # Generate filename
    target_date_str = target_date.strftime('%Y-%m-%d')
    timezone_suffix = "UTC" if use_utc_timezone else "ShopTimezone"
    filename = f"daily_orders_{target_date_str}_{timezone_suffix}.csv"
    
    # Write to CSV
    df.to_csv(filename, index=False)
    print(f"📄 Order details written to: {filename}")
    
    # Calculate summary statistics
    total_orders = len(df)
    total_sales = df['sales_net_sales'].sum()
    total_shopify_payments = df['payments_shopify_payments'].sum()
    total_payout_amount = df['shopify_amount_before_fees'].sum()
    total_difference = df['reconciliation_difference'].sum()
    mismatches = df['reconciliation_mismatch'].sum()
    
    # Display summary
    print(f"\n📋 DAILY SUMMARY for {target_date_str}:")
    print(f"   🌍 Timezone: {target_timezone}")
    print(f"   📦 Total Orders: {total_orders}")
    print(f"   💰 Total Net Sales: ${total_sales:.2f}")
    print(f"   🏪 Total Shopify Payments: ${total_shopify_payments:.2f}")
    print(f"   � Total Payout Amount: ${total_payout_amount:.2f}")
    print(f"   ⚖️  Total Difference: ${total_difference:.2f}")
    print(f"   🚨 Orders with Mismatches: {mismatches}")
    
    if mismatches > 0:
        print(f"\n⚠️  {mismatches} orders have reconciliation mismatches!")
        print("Check the CSV file for detailed analysis.")
        
        # Show orders with mismatches
        mismatch_orders = df[df['reconciliation_mismatch'] == True]
        print(f"\nOrders with mismatches:")
        for _, row in mismatch_orders.iterrows():
            print(f"  {row['order_name']}: ${row['reconciliation_difference']:.2f}")
    else:
        print(f"\n✅ All orders reconciled successfully!")
    
    print(f"\n✅ Daily reconciliation complete. Results saved to {filename}")

if __name__ == "__main__":
    main()

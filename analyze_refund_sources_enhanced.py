# analyze_refund_sources_enhanced.py
# Comprehensive analysis of all FOUR refund data sources in Shopify orders

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

def fetch_orders_for_refund_analysis(start_date, end_date):
    """Fetch orders with comprehensive refund data"""
    
    # Build the query string for date filtering
    date_query = f"created_at:>={start_date}T00:00:00Z AND created_at:<={end_date}T23:59:59Z"

    # Enhanced GraphQL query focusing on all refund sources
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
                totalRefundedSet { presentmentMoney { amount currencyCode } }
                
                # Source 1: Order transactions (including REFUND kinds)
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
                
                # Source 2: Refund objects with their own dates and line items
                refunds {
                    id
                    createdAt
                    note
                    refundLineItems(first: 50) {
                        nodes {
                            id
                            quantity
                            subtotalSet {
                                shopMoney {
                                    amount
                                    currencyCode
                                }
                            }
                        }
                    }
                    # Source 3: Refund transactions (nested within refund objects)
                    transactions(first: 50) {
                        nodes {
                            id
                            kind
                            gateway
                            status
                            createdAt
                            processedAt
                            amountSet {
                                shopMoney {
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

    print(f"🔍 Fetching orders for refund analysis from {start_date} to {end_date}")
    
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

    return orders

def analyze_refund_sources(orders):
    """Analyze all four refund sources for each order"""
    
    refund_analysis = []
    orders_with_refunds = 0
    
    print(f"\n🔍 Analyzing refund sources across {len(orders)} orders...")
    
    for order in orders:
        order_name = order['name']
        order_created = pd.to_datetime(order['createdAt'])
        order_processed = pd.to_datetime(order['processedAt']) if order['processedAt'] else order_created
        
        # SOURCE 1: Order-level total refunded amount
        total_refunded_amount = float(order['totalRefundedSet']['presentmentMoney']['amount'])
        
        # Skip orders with no refunds
        if total_refunded_amount == 0 and not order.get('refunds', []):
            continue
            
        orders_with_refunds += 1
        
        print(f"\n📋 Order {order_name} (Created: {order_created.strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"   💰 Total Refunded Amount: ${total_refunded_amount:.2f}")
        
        # SOURCE 2: Transaction-level refunds (kind='REFUND')
        transaction_refunds = []
        for txn in order['transactions']:
            if txn['kind'] == 'REFUND' and txn['status'] == 'SUCCESS':
                refund_amount = float(txn['amountSet']['presentmentMoney']['amount'])
                txn_created = pd.to_datetime(txn['createdAt'])
                txn_processed = pd.to_datetime(txn['processedAt'])
                
                transaction_refunds.append({
                    'transaction_id': txn['id'],
                    'amount': refund_amount,
                    'gateway': txn['gateway'],
                    'created_at': txn_created,
                    'processed_at': txn_processed,
                    'test': txn.get('test', False)
                })
                
                print(f"   🔄 Transaction Refund: ${refund_amount:.2f} via {txn['gateway']} (Processed: {txn_processed.strftime('%Y-%m-%d %H:%M:%S')})")
        
        # SOURCE 3: Refund object line items
        refund_line_items = []
        # SOURCE 4: Refund object transactions
        refund_object_transactions = []
        
        for refund in order.get('refunds', []):
            refund_id = refund['id']
            refund_created = pd.to_datetime(refund['createdAt'])
            refund_note = refund.get('note', '')
            
            print(f"   📦 Refund Object {refund_id} (Created: {refund_created.strftime('%Y-%m-%d %H:%M:%S')})")
            if refund_note:
                print(f"       Note: {refund_note}")
            
            # SOURCE 3: Line items within this refund
            line_item_total = 0
            for line_item in refund.get('refundLineItems', {}).get('nodes', []):
                line_amount = float(line_item['subtotalSet']['shopMoney']['amount'])
                line_item_total += line_amount
                
                refund_line_items.append({
                    'refund_id': refund_id,
                    'line_item_id': line_item['id'],
                    'quantity': line_item['quantity'],
                    'amount': line_amount,
                    'refund_created_at': refund_created
                })
                
                print(f"       📄 Line Item: ${line_amount:.2f} (Qty: {line_item['quantity']})")
            
            print(f"       📊 Total Line Items: ${line_item_total:.2f}")
            
            # SOURCE 4: Transactions within this refund object
            refund_txn_total = 0
            for refund_txn in refund.get('transactions', {}).get('nodes', []):
                if refund_txn['status'] == 'SUCCESS':
                    refund_txn_amount = float(refund_txn['amountSet']['shopMoney']['amount'])
                    refund_txn_created = pd.to_datetime(refund_txn['createdAt'])
                    refund_txn_processed = pd.to_datetime(refund_txn['processedAt'])
                    refund_txn_total += refund_txn_amount
                    
                    refund_object_transactions.append({
                        'refund_id': refund_id,
                        'transaction_id': refund_txn['id'],
                        'kind': refund_txn['kind'],
                        'gateway': refund_txn['gateway'],
                        'amount': refund_txn_amount,
                        'created_at': refund_txn_created,
                        'processed_at': refund_txn_processed,
                        'refund_created_at': refund_created
                    })
                    
                    print(f"       🔄 Refund Transaction: ${refund_txn_amount:.2f} via {refund_txn['gateway']} (Kind: {refund_txn['kind']}, Processed: {refund_txn_processed.strftime('%Y-%m-%d %H:%M:%S')})")
            
            print(f"       📊 Total Refund Transactions: ${refund_txn_total:.2f}")
        
        # Calculate totals for comparison
        total_transaction_refunds = sum(r['amount'] for r in transaction_refunds)
        total_line_items = sum(r['amount'] for r in refund_line_items)
        total_refund_transactions = sum(r['amount'] for r in refund_object_transactions)
        
        print(f"\n   📊 SUMMARY COMPARISON:")
        print(f"   1. Order Total Refunded: ${total_refunded_amount:.2f} (Uses order created/processed dates)")
        print(f"   2. Transaction Refunds: ${total_transaction_refunds:.2f} (Uses transaction processed dates)")
        print(f"   3. Refund Line Items: ${total_line_items:.2f} (Uses refund created dates)")
        print(f"   4. Refund Object Transactions: ${total_refund_transactions:.2f} (Uses transaction processed dates)")
        
        # Check for discrepancies
        discrepancies = []
        if abs(total_refunded_amount - total_transaction_refunds) > 0.01:
            discrepancies.append(f"Order total vs Transaction refunds: ${abs(total_refunded_amount - total_transaction_refunds):.2f}")
        if abs(total_refunded_amount - total_line_items) > 0.01:
            discrepancies.append(f"Order total vs Line items: ${abs(total_refunded_amount - total_line_items):.2f}")
        if abs(total_refunded_amount - total_refund_transactions) > 0.01:
            discrepancies.append(f"Order total vs Refund transactions: ${abs(total_refunded_amount - total_refund_transactions):.2f}")
        if abs(total_transaction_refunds - total_refund_transactions) > 0.01:
            discrepancies.append(f"Transaction refunds vs Refund transactions: ${abs(total_transaction_refunds - total_refund_transactions):.2f}")
        
        if discrepancies:
            print(f"   ⚠️  DISCREPANCIES FOUND:")
            for disc in discrepancies:
                print(f"     • {disc}")
        else:
            print(f"   ✅ All refund sources match!")
        
        # Store detailed analysis
        refund_analysis.append({
            'order_name': order_name,
            'order_created_at': order_created,
            'order_processed_at': order_processed,
            'total_refunded_amount': total_refunded_amount,
            'transaction_refunds': transaction_refunds,
            'refund_line_items': refund_line_items,
            'refund_object_transactions': refund_object_transactions,
            'total_transaction_refunds': total_transaction_refunds,
            'total_line_items': total_line_items,
            'total_refund_transactions': total_refund_transactions,
            'has_discrepancies': len(discrepancies) > 0,
            'discrepancies': discrepancies
        })
    
    print(f"\n📊 ANALYSIS COMPLETE: Found {orders_with_refunds} orders with refunds out of {len(orders)} total orders")
    return refund_analysis

def generate_refund_summary(refund_analysis):
    """Generate comprehensive summary of refund sources and their timing"""
    
    print(f"\n📋 REFUND SOURCES SUMMARY")
    print("=" * 60)
    
    # Overall statistics
    total_orders_with_refunds = len(refund_analysis)
    orders_with_discrepancies = sum(1 for analysis in refund_analysis if analysis['has_discrepancies'])
    
    print(f"📊 Overall Statistics:")
    print(f"   • Orders with refunds: {total_orders_with_refunds}")
    print(f"   • Orders with discrepancies: {orders_with_discrepancies}")
    print(f"   • Accuracy rate: {((total_orders_with_refunds - orders_with_discrepancies) / total_orders_with_refunds * 100):.1f}%")
    
    # Date grouping analysis
    print(f"\n📅 DATE GROUPING ANALYSIS:")
    print("Understanding when to group refunds by different dates...")
    
    date_comparisons = defaultdict(list)
    
    for analysis in refund_analysis:
        order_name = analysis['order_name']
        order_date = analysis['order_created_at'].date()
        
        # Collect all unique dates from different sources
        dates = {
            'order_created': order_date,
            'order_processed': analysis['order_processed_at'].date()
        }
        
        # Transaction refund dates
        for txn_refund in analysis['transaction_refunds']:
            txn_date = txn_refund['processed_at'].date()
            dates['transaction_processed'] = txn_date
        
        # Refund object dates
        for refund_item in analysis['refund_line_items']:
            refund_date = refund_item['refund_created_at'].date()
            dates['refund_created'] = refund_date
        
        # Refund transaction dates
        for refund_txn in analysis['refund_object_transactions']:
            refund_txn_date = refund_txn['processed_at'].date()
            dates['refund_transaction_processed'] = refund_txn_date
        
        # Analyze date spread
        unique_dates = set(dates.values())
        date_spread = max(unique_dates) - min(unique_dates) if len(unique_dates) > 1 else timedelta(0)
        
        date_comparisons[order_name] = {
            'dates': dates,
            'unique_dates': unique_dates,
            'date_spread_days': date_spread.days,
            'total_refund_amount': analysis['total_refunded_amount']
        }
    
    # Analyze date patterns
    same_date_orders = sum(1 for comp in date_comparisons.values() if comp['date_spread_days'] == 0)
    different_date_orders = total_orders_with_refunds - same_date_orders
    max_spread = max((comp['date_spread_days'] for comp in date_comparisons.values()), default=0)
    avg_spread = sum(comp['date_spread_days'] for comp in date_comparisons.values()) / total_orders_with_refunds
    
    print(f"   • Orders where all refund dates match: {same_date_orders}")
    print(f"   • Orders where refund dates differ: {different_date_orders}")
    print(f"   • Maximum date spread: {max_spread} days")
    print(f"   • Average date spread: {avg_spread:.1f} days")
    
    # Recommendations for date grouping
    print(f"\n💡 RECOMMENDATIONS FOR DATE GROUPING:")
    
    if same_date_orders > different_date_orders:
        print(f"   ✅ Most refunds occur on the same date as the order")
        print(f"   📅 Recommendation: Group by order creation date for simplicity")
    else:
        print(f"   ⚠️  Many refunds occur on different dates than the order")
        print(f"   📅 Recommendation: Consider grouping by actual refund/transaction dates")
    
    if max_spread > 7:
        print(f"   ⚠️  Some refunds are processed {max_spread} days after order creation")
        print(f"   📅 Important: Using order date may misalign refunds with cash flow")
    
    # Source comparison analysis
    print(f"\n🔍 REFUND SOURCE ANALYSIS:")
    
    source_matches = {
        'order_vs_transaction': 0,
        'order_vs_line_items': 0,
        'order_vs_refund_transactions': 0,
        'transaction_vs_refund_transactions': 0
    }
    
    for analysis in refund_analysis:
        total_order = analysis['total_refunded_amount']
        total_txn = analysis['total_transaction_refunds']
        total_line = analysis['total_line_items']
        total_refund_txn = analysis['total_refund_transactions']
        
        if abs(total_order - total_txn) <= 0.01:
            source_matches['order_vs_transaction'] += 1
        if abs(total_order - total_line) <= 0.01:
            source_matches['order_vs_line_items'] += 1
        if abs(total_order - total_refund_txn) <= 0.01:
            source_matches['order_vs_refund_transactions'] += 1
        if abs(total_txn - total_refund_txn) <= 0.01:
            source_matches['transaction_vs_refund_transactions'] += 1
    
    print(f"   • Order total matches transaction refunds: {source_matches['order_vs_transaction']}/{total_orders_with_refunds}")
    print(f"   • Order total matches line items: {source_matches['order_vs_line_items']}/{total_orders_with_refunds}")
    print(f"   • Order total matches refund transactions: {source_matches['order_vs_refund_transactions']}/{total_orders_with_refunds}")
    print(f"   • Transaction refunds match refund transactions: {source_matches['transaction_vs_refund_transactions']}/{total_orders_with_refunds}")
    
    # Best source recommendation
    print(f"\n🎯 BEST REFUND SOURCE RECOMMENDATION:")
    
    if source_matches['transaction_vs_refund_transactions'] == total_orders_with_refunds:
        print(f"   ✅ Transaction refunds and refund transactions are identical")
        print(f"   📊 Use either source - they provide the same data")
        print(f"   📅 Refund transactions may provide more precise dates")
    elif source_matches['order_vs_transaction'] == total_orders_with_refunds:
        print(f"   ✅ Order totals and transaction refunds match perfectly")
        print(f"   📊 Use transaction refunds for precise timing")
    else:
        print(f"   ⚠️  Sources don't match perfectly - investigate discrepancies")
        print(f"   📊 Recommend manual review of mismatched orders")
    
    return date_comparisons, source_matches

def export_detailed_analysis(refund_analysis, date_comparisons):
    """Export detailed analysis to CSV files"""
    
    print(f"\n💾 Exporting detailed analysis...")
    
    # Export 1: Order-level summary
    order_summary = []
    for analysis in refund_analysis:
        order_summary.append({
            'order_name': analysis['order_name'],
            'order_created_date': analysis['order_created_at'].strftime('%Y-%m-%d'),
            'order_created_datetime': analysis['order_created_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'order_processed_datetime': analysis['order_processed_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'total_refunded_amount': analysis['total_refunded_amount'],
            'transaction_refunds_total': analysis['total_transaction_refunds'],
            'line_items_total': analysis['total_line_items'],
            'refund_transactions_total': analysis['total_refund_transactions'],
            'has_discrepancies': analysis['has_discrepancies'],
            'discrepancy_details': '; '.join(analysis['discrepancies']) if analysis['discrepancies'] else '',
            'date_spread_days': date_comparisons[analysis['order_name']]['date_spread_days']
        })
    
    df_summary = pd.DataFrame(order_summary)
    df_summary.to_csv('refund_analysis_summary.csv', index=False)
    print(f"   📋 Exported order summary: refund_analysis_summary.csv")
    
    # Export 2: Transaction-level details
    transaction_details = []
    for analysis in refund_analysis:
        order_name = analysis['order_name']
        
        # Add transaction refunds
        for txn in analysis['transaction_refunds']:
            transaction_details.append({
                'order_name': order_name,
                'source': 'transaction_refund',
                'transaction_id': txn['transaction_id'],
                'refund_id': '',
                'amount': txn['amount'],
                'gateway': txn['gateway'],
                'created_date': txn['created_at'].strftime('%Y-%m-%d'),
                'created_datetime': txn['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'processed_date': txn['processed_at'].strftime('%Y-%m-%d'),
                'processed_datetime': txn['processed_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'is_test': txn['test']
            })
        
        # Add refund object transactions
        for refund_txn in analysis['refund_object_transactions']:
            transaction_details.append({
                'order_name': order_name,
                'source': 'refund_object_transaction',
                'transaction_id': refund_txn['transaction_id'],
                'refund_id': refund_txn['refund_id'],
                'amount': refund_txn['amount'],
                'gateway': refund_txn['gateway'],
                'created_date': refund_txn['created_at'].strftime('%Y-%m-%d'),
                'created_datetime': refund_txn['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'processed_date': refund_txn['processed_at'].strftime('%Y-%m-%d'),
                'processed_datetime': refund_txn['processed_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'is_test': False
            })
    
    df_transactions = pd.DataFrame(transaction_details)
    df_transactions.to_csv('refund_transaction_details.csv', index=False)
    print(f"   🔄 Exported transaction details: refund_transaction_details.csv")
    
    # Export 3: Date grouping analysis
    grouping_analysis = []
    for order_name, comp in date_comparisons.items():
        dates = comp['dates']
        grouping_analysis.append({
            'order_name': order_name,
            'order_created_date': dates['order_created'].strftime('%Y-%m-%d'),
            'order_processed_date': dates['order_processed'].strftime('%Y-%m-%d'),
            'transaction_processed_date': dates.get('transaction_processed', '').strftime('%Y-%m-%d') if dates.get('transaction_processed') else '',
            'refund_created_date': dates.get('refund_created', '').strftime('%Y-%m-%d') if dates.get('refund_created') else '',
            'refund_transaction_processed_date': dates.get('refund_transaction_processed', '').strftime('%Y-%m-%d') if dates.get('refund_transaction_processed') else '',
            'unique_dates_count': len(comp['unique_dates']),
            'date_spread_days': comp['date_spread_days'],
            'total_refund_amount': comp['total_refund_amount']
        })
    
    df_grouping = pd.DataFrame(grouping_analysis)
    df_grouping.to_csv('refund_date_grouping_analysis.csv', index=False)
    print(f"   📅 Exported date grouping analysis: refund_date_grouping_analysis.csv")
    
    print(f"   ✅ All analysis files exported successfully!")

def main():
    print("🔍 SHOPIFY REFUND SOURCES ANALYSIS")
    print("=" * 50)
    print("Analyzing all FOUR refund data sources:")
    print("1. Order totalRefundedSet (aggregate)")
    print("2. Transaction refunds (kind='REFUND')")
    print("3. Refund object line items")
    print("4. Refund object transactions")
    print()
    
    # Set date range: This year up to two days ago
    end_date = datetime.now().date() - timedelta(days=2)
    start_date = date(end_date.year, 1, 1)
    
    print(f"📅 Analysis period: {start_date} to {end_date}")
    print(f"   (This year up to two days ago)")
    print()
    
    # Fetch orders
    orders = fetch_orders_for_refund_analysis(start_date, end_date)
    
    # Analyze refund sources
    refund_analysis = analyze_refund_sources(orders)
    
    # Generate summary and recommendations
    date_comparisons, source_matches = generate_refund_summary(refund_analysis)
    
    # Export detailed analysis
    export_detailed_analysis(refund_analysis, date_comparisons)
    
    print(f"\n✅ REFUND ANALYSIS COMPLETE!")
    print(f"📊 Check the exported CSV files for detailed findings")

if __name__ == "__main__":
    main()

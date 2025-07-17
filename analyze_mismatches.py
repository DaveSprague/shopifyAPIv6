#!/usr/bin/env python3

import pandas as pd
import sys

def analyze_mismatches():
    """Analyze the reconciliation mismatches to understand patterns"""
    
    # Read the reconciliation file
    df = pd.read_csv('daily_sales_reconciliation_ShopTimezone_payout_grouped.csv')
    
    # Filter to only mismatches
    mismatches = df[df['reconciliation_mismatch'] == True].copy()
    
    print("MISMATCH ANALYSIS")
    print("=" * 60)
    print(f"Total days with mismatches: {len(mismatches)}")
    print(f"Total days in dataset: {len(df)}")
    print(f"Mismatch percentage: {len(mismatches)/len(df)*100:.1f}%")
    print()
    
    # Show the biggest mismatches
    print("TOP 10 LARGEST MISMATCHES (by absolute difference):")
    print("-" * 60)
    mismatches['abs_diff'] = mismatches['reconciliation_difference'].abs()
    top_mismatches = mismatches.nlargest(10, 'abs_diff')
    
    for _, row in top_mismatches.iterrows():
        print(f"Date: {row['date']}")
        print(f"  Orders: {row['order_count']}")
        print(f"  Shopify Payments (transactions): ${row['payments_shopify_payments']:,.2f}")
        print(f"  Shopify Amount Before Fees (payouts): ${row['shopify_amount_before_fees']:,.2f}")
        print(f"  Difference: ${row['reconciliation_difference']:,.2f}")
        print(f"  Earliest Order: {row['earliest_order_name']} ({row['earliest_order_created_at'][:10]})")
        print(f"  Latest Order: {row['latest_order_name']} ({row['latest_order_created_at'][:10]})")
        print(f"  Payout Count: {row['payout_count']}")
        print()
    
    # Analyze patterns
    print("PATTERN ANALYSIS:")
    print("-" * 30)
    
    # Days with very high sales but low payouts (multiple days grouped into one)
    high_sales_low_payout = mismatches[
        (mismatches['payments_shopify_payments'] > 5000) & 
        (mismatches['shopify_amount_before_fees'] < 2000)
    ]
    print(f"Days with high sales (>$5000) but low payouts (<$2000): {len(high_sales_low_payout)}")
    
    # Days with very low sales but high payouts (sales spread across multiple days)
    low_sales_high_payout = mismatches[
        (mismatches['payments_shopify_payments'] < 1000) & 
        (mismatches['shopify_amount_before_fees'] > 5000)
    ]
    print(f"Days with low sales (<$1000) but high payouts (>$5000): {len(low_sales_high_payout)}")
    
    # Days with multiple orders spanning multiple creation dates
    multi_day_orders = mismatches[mismatches['earliest_order_created_at'].str[:10] != mismatches['latest_order_created_at'].str[:10]]
    print(f"Days with orders spanning multiple creation dates: {len(multi_day_orders)}")
    
    # Show some examples of multi-day grouping
    print("\nEXAMPLES OF MULTI-DAY ORDER GROUPING:")
    print("-" * 40)
    for _, row in multi_day_orders.head(5).iterrows():
        earliest_date = row['earliest_order_created_at'][:10]
        latest_date = row['latest_order_created_at'][:10]
        print(f"Payout Date: {row['date']}")
        print(f"  Order dates span: {earliest_date} to {latest_date}")
        print(f"  Order count: {row['order_count']}")
        print(f"  Payments: ${row['payments_shopify_payments']:,.2f}")
        print(f"  Payouts: ${row['shopify_amount_before_fees']:,.2f}")
        print()

if __name__ == "__main__":
    analyze_mismatches()

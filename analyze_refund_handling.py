#!/usr/bin/env python3
"""
Test script to analyze how returns/refunds are handled in the reconciliation system
"""

import pandas as pd
import os
from datetime import datetime

def analyze_refund_handling():
    """Analyze how returns/refunds are currently handled in the reconciliation system"""
    
    print("REFUND/RETURN ANALYSIS")
    print("=" * 50)
    
    # Look for recent reconciliation output files
    output_files = []
    for filename in os.listdir('.'):
        if filename.startswith('daily_sales_reconciliation_') and filename.endswith('.csv'):
            output_files.append(filename)
    
    if not output_files:
        print("No reconciliation output files found. Please run reconcile_payouts.py first.")
        return
    
    # Use the most recent file
    latest_file = max(output_files, key=lambda f: os.path.getmtime(f))
    print(f"Analyzing: {latest_file}")
    
    # Load the reconciliation data
    try:
        df = pd.read_csv(latest_file)
        print(f"Loaded {len(df)} days of reconciliation data")
    except Exception as e:
        print(f"Error loading file: {e}")
        return
    
    # Check what refund-related columns exist
    refund_columns = [col for col in df.columns if 'refund' in col.lower()]
    print(f"\nRefund-related columns found: {refund_columns}")
    
    # Analyze refund data
    print("\nREFUND DATA ANALYSIS:")
    print("-" * 30)
    
    # Check order refunds (totalRefundedSet from orders)
    if 'payments_total_refunds' in df.columns:
        total_order_refunds = df['payments_total_refunds'].sum()
        days_with_order_refunds = (df['payments_total_refunds'] > 0).sum()
        print(f"Total order refunds (from orders): ${total_order_refunds:,.2f}")
        print(f"Days with order refunds: {days_with_order_refunds}")
    
    # Check payout refunds (from payout CSV)
    if 'shopify_payout_refunds' in df.columns:
        total_payout_refunds = df['shopify_payout_refunds'].sum()
        days_with_payout_refunds = (df['shopify_payout_refunds'] > 0).sum()
        print(f"Total payout refunds (from payout CSV): ${total_payout_refunds:,.2f}")
        print(f"Days with payout refunds: {days_with_payout_refunds}")
    
    # Check transaction refunds by gateway
    gateway_refunds = {}
    for col in refund_columns:
        if col.startswith('payments_') and col.endswith('_refunds') and col != 'payments_total_refunds':
            gateway = col.replace('payments_', '').replace('_refunds', '')
            total = df[col].sum()
            days = (df[col] > 0).sum()
            if total > 0:
                gateway_refunds[gateway] = {'total': total, 'days': days}
    
    if gateway_refunds:
        print(f"\nRefunds by payment gateway:")
        for gateway, data in gateway_refunds.items():
            print(f"  {gateway}: ${data['total']:,.2f} across {data['days']} days")
    
    # Check how refunds affect reconciliation
    print(f"\nREFUND IMPACT ON RECONCILIATION:")
    print("-" * 40)
    
    # Look at days with significant refunds
    if 'payments_total_refunds' in df.columns and 'shopify_payout_refunds' in df.columns:
        refund_comparison = df[['date', 'payments_total_refunds', 'shopify_payout_refunds', 
                               'reconciliation_difference', 'reconciliation_mismatch']].copy()
        
        # Filter to days with refunds
        refund_days = refund_comparison[
            (refund_comparison['payments_total_refunds'] > 0) | 
            (refund_comparison['shopify_payout_refunds'] > 0)
        ]
        
        if len(refund_days) > 0:
            print(f"Found {len(refund_days)} days with refunds:")
            print(refund_days.head(10).to_string(index=False))
            
            # Check if refunds are causing mismatches
            refund_mismatches = refund_days[refund_days['reconciliation_mismatch'] == True]
            if len(refund_mismatches) > 0:
                print(f"\n{len(refund_mismatches)} days with refunds have reconciliation mismatches:")
                print(refund_mismatches[['date', 'payments_total_refunds', 'shopify_payout_refunds', 
                                       'reconciliation_difference']].to_string(index=False))
    
    # Check the current reconciliation logic
    print(f"\nCURRENT RECONCILIATION LOGIC:")
    print("-" * 35)
    print("According to the code analysis:")
    print("1. Order refunds are tracked via totalRefundedSet field")
    print("2. Transaction refunds are tracked by gateway type (shopify_payments_refunds, cash_refunds, etc.)")
    print("3. Payout refunds are tracked separately from payout CSV file")
    print("4. Reconciliation compares 'payments_shopify_payments' vs 'shopify_amount_before_fees'")
    print("5. The comment says 'This is already net of refunds from order processing'")
    
    # Check if this is actually true
    if 'payments_shopify_payments' in df.columns and 'payments_shopify_refunds' in df.columns:
        print(f"\nVERIFYING REFUND HANDLING:")
        print("-" * 30)
        
        # Look at a sample of data
        sample_data = df[['date', 'payments_shopify_payments', 'payments_shopify_refunds', 
                         'shopify_amount_before_fees', 'shopify_payout_refunds']].head(10)
        print("Sample data (first 10 days):")
        print(sample_data.to_string(index=False))
        
        # Check if payments_shopify_payments includes refunds or is net of refunds
        total_payments = df['payments_shopify_payments'].sum()
        total_refunds = df['payments_shopify_refunds'].sum()
        print(f"\nTotal Shopify payments: ${total_payments:,.2f}")
        print(f"Total Shopify refunds: ${total_refunds:,.2f}")
        
        # This suggests whether refunds are already netted or not
        if total_refunds > 0:
            print("\nWARNING: Separate refund tracking suggests refunds may NOT be netted from payments!")
            print("This could cause reconciliation issues if refunds are double-counted.")
    
    print(f"\nRECOMMENDATIONS:")
    print("-" * 20)
    print("1. Verify if Shopify payments are gross or net of refunds")
    print("2. Ensure refunds are properly netted from the order side reconciliation")
    print("3. Check if payout refunds are separate transactions or adjustments")
    print("4. Consider creating a dedicated refund reconciliation report")

if __name__ == "__main__":
    analyze_refund_handling()

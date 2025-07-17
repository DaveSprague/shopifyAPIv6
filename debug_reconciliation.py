import pandas as pd
from collections import defaultdict

# Load the payout CSV
payout_df = pd.read_csv('payoutTransactionFiles/YTD_2025_payment_transactions_export_1.csv')

# Convert dates to datetime
payout_df['Payout Date'] = pd.to_datetime(payout_df['Payout Date'])

# Test for a specific date - let's look at one that shows mismatch
test_date = pd.to_datetime('2025-07-15')
test_data = payout_df[payout_df['Payout Date'] == test_date]

print(f"=== TESTING PAYOUT DATE: {test_date.strftime('%Y-%m-%d')} ===")
print(f"Number of transactions: {len(test_data)}")
print(f"Total payout amount: ${test_data['Amount'].sum():.2f}")
print(f"Total payout fees: ${test_data['Fee'].sum():.2f}")
print(f"Total net amount: ${test_data['Net'].sum():.2f}")
print()

# Show the individual orders
print("Individual transactions:")
for _, row in test_data.head(10).iterrows():
    print(f"  Order {row['Order']}: ${row['Amount']:.2f} (fee: ${row['Fee']:.2f}, net: ${row['Net']:.2f})")
print()

# Now let's check what our reconciliation system thinks
reconciliation_df = pd.read_csv('daily_sales_reconciliation_UTC_payout_grouped.csv')
reconciliation_row = reconciliation_df[reconciliation_df['date'] == test_date.strftime('%Y-%m-%d')]

if len(reconciliation_row) > 0:
    row = reconciliation_row.iloc[0]
    print(f"=== RECONCILIATION SYSTEM RESULTS FOR 2025-07-15 ===")
    print(f"Order count: {row['order_count']}")
    print(f"Shopify payments (from orders): ${row['payments_shopify_payments']:.2f}")
    print(f"Shopify amount before fees (from payout): ${row['shopify_amount_before_fees']:.2f}")
    print(f"Shopify net deposit (from payout): ${row['shopify_net_deposit']:.2f}")
    print(f"Payout count: {row['payout_count']}")
    print(f"Reconciliation total receipts: ${row['reconciliation_total_receipts']:.2f}")
    print(f"Reconciliation expected payout: ${row['reconciliation_expected_payout']:.2f}")
    print(f"Reconciliation difference: ${row['reconciliation_difference']:.2f}")
    print(f"Reconciliation mismatch: {row['reconciliation_mismatch']}")
    print()
    
    print("=== COMPARISON ===")
    print(f"Payout CSV total amount: ${test_data['Amount'].sum():.2f}")
    print(f"Reconciliation expected payout: ${row['reconciliation_expected_payout']:.2f}")
    print(f"Difference: ${test_data['Amount'].sum() - row['reconciliation_expected_payout']:.2f}")
    print()
    
    print(f"Orders from payout CSV: {len(test_data)} transactions")
    print(f"Orders from reconciliation: {row['order_count']} orders")
else:
    print("No reconciliation data found for 2025-07-15")

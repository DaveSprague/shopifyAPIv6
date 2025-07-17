import pandas as pd

# Test July 11, 2025 reconciliation
df = pd.read_csv('daily_sales_reconciliation_UTC_payout_grouped.csv')
row = df[df['date'] == '2025-07-11'].iloc[0]

print("July 11, 2025 reconciliation results:")
print(f"Order count: {row['order_count']}")
print(f"Shopify payments (from orders): ${row['payments_shopify_payments']:.2f}")
print(f"Shopify amount before fees (from payout): ${row['shopify_amount_before_fees']:.2f}")
print(f"Shopify net deposit (from payout): ${row['shopify_net_deposit']:.2f}")
print(f"Payout count: {row['payout_count']}")
print(f"Reconciliation difference: ${row['reconciliation_difference']:.2f}")
print(f"Mismatch: {row['reconciliation_mismatch']}")

# Verify against payout CSV
payout_df = pd.read_csv('payoutTransactionFiles/YTD_2025_payment_transactions_export_1.csv')
payout_df['Payout Date'] = pd.to_datetime(payout_df['Payout Date'])
july11_paid = payout_df[(payout_df['Payout Date'].dt.date == pd.to_datetime('2025-07-11').date()) & (payout_df['Payout Status'] == 'paid')]

print()
print("Payout CSV verification:")
print(f"Paid transactions on July 11: {len(july11_paid)}")
print(f"Total payout amount: ${july11_paid['Amount'].sum():.2f}")
print(f"Total fees: ${july11_paid['Fee'].sum():.2f}")
print(f"Net deposit: ${july11_paid['Net'].sum():.2f}")

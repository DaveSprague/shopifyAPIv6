import pandas as pd

# Read the CSV file
df = pd.read_csv('daily_sales_reconciliation_ShopTimezone_payout_grouped.csv')

print("=== RECONCILIATION ANALYSIS ===")
print(f"Total rows: {len(df)}")
print(f"Mismatches: {df['reconciliation_mismatch'].sum()}")
print(f"Perfect matches: {(~df['reconciliation_mismatch']).sum()}")
print(f"Match rate: {((~df['reconciliation_mismatch']).sum() / len(df)) * 100:.1f}%")

print("\n=== SAMPLE RECONCILIATION DATA ===")
print("Date | Receipts | Expected Payout | Difference | Mismatch")
print("-" * 60)
for _, row in df.head(10).iterrows():
    print(f"{row['date']} | ${row['reconciliation_total_receipts']:.2f} | ${row['reconciliation_expected_payout']:.2f} | ${row['reconciliation_difference']:.2f} | {row['reconciliation_mismatch']}")

print("\n=== EXAMINING THE CORE ISSUE ===")
print("Looking at the data structure:")
print(f"- Orders are being grouped by PAYOUT date, not order date")
print(f"- But we're still seeing massive mismatches")
print(f"- Let's examine a few key columns:")

print("\n=== KEY COLUMNS ANALYSIS ===")
sample_row = df.iloc[0]
print(f"Sample row (date: {sample_row['date']}):")
print(f"- Order count: {sample_row['order_count']}")
print(f"- Shopify payments: ${sample_row['payments_shopify_payments']:.2f}")
print(f"- Shopify amount before fees: ${sample_row['shopify_amount_before_fees']:.2f}")
print(f"- Shopify net deposit: ${sample_row['shopify_net_deposit']:.2f}")
print(f"- Payout count: {sample_row['payout_count']}")
print(f"- Reconciliation total receipts: ${sample_row['reconciliation_total_receipts']:.2f}")
print(f"- Reconciliation expected payout: ${sample_row['reconciliation_expected_payout']:.2f}")
print(f"- Reconciliation difference: ${sample_row['reconciliation_difference']:.2f}")

print("\n=== POTENTIAL PROBLEMS ===")
print("1. Are we comparing the right values?")
print("2. Are we grouping orders correctly by payout date?")
print("3. Are there timezone issues?")
print("4. Are we handling refunds correctly?")
print("5. Are we missing some transactions?")

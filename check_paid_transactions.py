import pandas as pd

# Load the payout CSV
df = pd.read_csv('payoutTransactionFiles/YTD_2025_payment_transactions_export_1.csv')

print("Payout statuses overall:")
print(df['Payout Status'].value_counts())
print()

print("Sample paid transactions:")
paid = df[df['Payout Status'] == 'paid']
if len(paid) > 0:
    print(paid.groupby('Payout Date').size().head())
    print()
    print("Testing a specific paid payout date:")
    sample_date = paid['Payout Date'].iloc[0]
    print(f"Date: {sample_date}")
    sample_data = paid[paid['Payout Date'] == sample_date]
    print(f"Number of transactions: {len(sample_data)}")
    print(f"Total amount: ${sample_data['Amount'].sum():.2f}")
else:
    print("No paid transactions found")

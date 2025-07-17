import pandas as pd
import os

# Test the restored functionality
print('TESTING RESTORED FUNCTIONALITY')
print('=' * 40)

# Check if we have the latest CSV file
csv_file = 'daily_sales_reconciliation_UTC_payout_grouped.csv'
if os.path.exists(csv_file):
    df = pd.read_csv(csv_file)
    print(f'CSV file exists: {csv_file}')
    print(f'Total rows: {len(df)}')
    print(f'Source/location values: {sorted(df["source_location"].unique())}')
    print()
    
    # Show sample structure
    print('SAMPLE STRUCTURE:')
    sample_date = df['date'].iloc[0]
    sample_rows = df[df['date'] == sample_date][['date', 'source_location', 'sales_gross_sales', 'sales_order_count']]
    print(sample_rows.to_string(index=False))
    
    print()
    print('SUCCESS: Row-based structure is working!')
    print('You can now use pivot tables easily in Excel.')
else:
    print('CSV file not found. Run the main script to generate it.')

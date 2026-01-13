import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ==========================================
# STEP 1: GENERATE DUMMY DATA (The "Source")
# ==========================================
def generate_mock_data():
    # Create Inventory Data
    inventory_data = {
        'Product_ID': [101, 102, 103, 104, 105],
        'Product_Name': ['Wireless Mouse', 'Gaming Keyboard', 'USB-C Cable', 'Monitor Stand', 'Webcam'],
        'Stock_On_Hand': [45, 12, 0, 25, 18],  # 104 and 105 will be our "Ghosts"
        'Store_ID': ['S001', 'S001', 'S001', 'S001', 'S001']
    }
    
    # Create Sales Data (Simulating 7 days of sales)
    # Notice: Product 104 and 105 are missing from sales (0 sales)
    sales_data = {
        'Product_ID': [101, 101, 102, 103, 101, 102],
        'Quantity_Sold': [2, 1, 1, 5, 2, 1],
        'Date': [(datetime.now() - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(6)]
    }

    pd.DataFrame(inventory_data).to_csv('inventory.csv', index=False)
    pd.DataFrame(sales_data).to_csv('sales_transactions.csv', index=False)
    print("--- Mock Data Files Created Successfully ---")

# ==========================================
# STEP 2: THE ETL PIPELINE (The "Logic")
# ==========================================
def run_audit_pipeline():
    # 1. Extraction (E)
    df_inv = pd.read_csv('inventory.csv')
    df_sales = pd.read_csv('sales_transactions.csv')

    # 2. Transformation (T)
    # Sum up sales for each product
    total_sales = df_sales.groupby('Product_ID')['Quantity_Sold'].sum().reset_index()
    
    # Merge inventory with sales data
    # We use 'left' join to keep all inventory items even if they had 0 sales
    merged_data = pd.merge(df_inv, total_sales, on='Product_ID', how='left')
    
    # Fill NaN values with 0 (meaning no sales were found)
    merged_data['Quantity_Sold'] = merged_data['Quantity_Sold'].fillna(0)

    # 3. Apply Business Logic (The "Audit Rule")
    # A 'Ghost' is: Stock > 5 units BUT Sales = 0
    condition = (merged_data['Stock_On_Hand'] > 5) & (merged_data['Quantity_Sold'] == 0)
    merged_data['Audit_Status'] = np.where(condition, 'FLAGGED: GHOST INVENTORY', 'OK')

    # 4. Loading/Output (L)
    audit_report = merged_data[merged_data['Audit_Status'] == 'FLAGGED: GHOST INVENTORY']
    audit_report.to_csv('final_audit_report.csv', index=False)
    
    print("\n--- Pipeline Run Complete ---")
    print(f"Total Products Checked: {len(merged_data)}")
    print(f"Ghosts Detected: {len(audit_report)}")
    print("\nResults Preview:")
    print(merged_data[['Product_Name', 'Stock_On_Hand', 'Quantity_Sold', 'Audit_Status']])

# Run the project
if __name__ == "__main__":
    generate_mock_data()
    run_audit_pipeline()
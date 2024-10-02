import pandas as pd
from sqlalchemy import create_engine

# Load data from both Excel files
df1 = pd.read_excel('product_O.xlsx')  # First file with pincode, area, city, store_id
df2 = pd.read_excel('pincode_data_new.xlsx')  # Second file with Id, brand_name, sku_name, product_name, etc.

# Clean up column names by stripping spaces and converting to lowercase
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# Preview the column names and data to ensure they match
print("Columns in df1:", df1.columns)
print("Columns in df2:", df2.columns)
print(df1.head())  # Check first few rows of df1
print(df2.head())  # Check first few rows of df2

# Initialize an empty DataFrame for the master table
pincode_data_new_O = pd.DataFrame()

# Iterate through each row of the first DataFrame
for index, row in df2.iterrows():
    # Extract relevant data from the first file (df1)
    city = row.get('city', 'NA')  # Use 'NA' if the column is missing
    request_pincode = row.get('request_pincode', 'NA')
    store_address = row.get('store_address', 'NA')
    store_fssai_license = row.get('store_fssai_license', 'NA')
    store_id = row.get('store_id', 'NA')  # Use 'NA' if the column is missing
    store_lat_long = row.get('request_pincode', 'NA')


    # Check if pincode, area, city, and store_id are being accessed correctly
    print(f"Processing row {index} from df1: {city}, {request_pincode}, {store_address}, {store_fssai_license},{store_id},{store_lat_long}")

    # Iterate through the second DataFrame
    for i, product_row in df1.iterrows():
        # Combine the current row from df1 and df2
        combined_data = {
            'city': city,
            'request_pincode':request_pincode,
            'store_address':store_address,
            'store_fssai_license':store_fssai_license,
            'store_id': store_id,
            'store_lat_long':store_lat_long,

            'product_id': product_row.get('product_id', 'NA'),
            'brand': product_row.get('brand', 'NA'),

        }

        # Check the combined row before adding it
        print(f"Combined data row {i}: {combined_data}")

        # Convert the dictionary to a DataFrame and concatenate with the master table
        pincode_data_new_O = pd.concat([pincode_data_new_O, pd.DataFrame([combined_data])], ignore_index=True)

# SQL database connection setup
engine = create_engine('mysql+pymysql://root:actowiz@localhost/swiggy_instamart')

# Store the master table into SQL
pincode_data_new_O.to_sql('pincode_data_new_O', con=engine, if_exists='replace', index=False)

print("Data successfully inserted into SQL!")

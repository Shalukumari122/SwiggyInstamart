import os
from datetime import datetime, timedelta

import pandas as pd
import pymysql

# Connect to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='swiggy_instamart'
)
# query ='select `id`,`pincode`,`lat`,`long`,`serviceable` from blinkit_lat_long_comp'
query ='select request_pincode from pincode_data_new'
df = pd.read_sql(query, conn)

# Close the database connection
conn.close()

output_file_path = 'pincode.xlsx'

# Create the directory if it does not exist
df.to_excel(output_file_path, index=False)
print(f"Data has been exported to {output_file_path}")



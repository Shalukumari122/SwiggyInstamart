import html
import os.path
from datetime import date, datetime

import pandas as pd
import sqlalchemy
import pymysql
from openpyxl import writer

conn = pymysql.connect(
    host='',
    user='root',
    password='actowiz',
    database='swiggy_instamart'
)
cursor = conn.cursor()


product_IDS_M_T = {
    'MyFitness': ['K0D81W05OY', 'SP3N22IA2S', '9956KXSZ5G', '6JTI43RCFM', '7JNY31RR6T',
                  '7MVKN2CXJD', 'AAD0OM5GLH', '0P05XCZ20H', '0YSSY1LH3L', 'AFHQMG5AV3'],
    'Pebble': ['NGI4IP74FG', 'DG52GCP0B2', 'QUIPWE1KRO'],
    'Party Propz': ['YZEEFEA4W8', 'W0VCR2BFSE', 'D7E5DNYSTR', '382R5UET79']
}

product_IDS_O = {'MyFitness': ['K0D81W05OY', 'SP3N22IA2S', '9956KXSZ5G', '6JTI43RCFM', '7JNY31RR6T',
                               '7MVKN2CXJD', 'AAD0OM5GLH', '0P05XCZ20H', '0YSSY1LH3L', 'AFHQMG5AV3']}


today_date = str(date.today()).replace('-', '_')

folder_loc = f'C:/Shalu/PageSave/SwiggyInstamart/page_save_{today_date}/'

oos = []

for brand, product_ids in product_IDS_O.items():

# for brand, product_ids in product_IDS_M_T.items():

    if brand == "Pebble":
        excluded_city = 'Pune'
    else:
        excluded_city = 'Lucknow'

    query = f"select * from  pincode_data_new where city!='{excluded_city}'"

    print(query)

    cursor.execute(query)

    all_pincodes = cursor.fetchall()

    for each_product in product_ids:

        for each_tup in all_pincodes:

            page_loc = folder_loc + each_tup[2] + f'/{each_product}.html'

            if not os.path.isfile(page_loc):
                oos_p = dict()
                oos_p['channel'] = 'Swiggy Instamart'
                oos_p['scraped_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                oos_p['city'] = each_tup[1]
                oos_p['request_pincode'] = each_tup[2]
                oos_p['product_id'] = each_product
                oos_p['product_url'] = f"https://www.swiggy.com/instamart/item/{each_product}?storeId={each_tup[-2]}"
                oos_p['brand'] = brand
                oos.append(oos_p)


today_date = str(date.today()).replace('-', '_')


data_folder_loc = f'C:/Shalu/LiveProjects/SwiggyInstamart/data_files/{today_date}/'
file = data_folder_loc + 'swiggy_instamart.json'
data = pd.read_json(file)
# oos = []
for each_p in data[data['stock_status'] == 'out of stock'].to_dict("records"):
# for each_p in data[data['stock_status'] == 0].to_dict("records"):
    oos_p = dict()
    oos_p['channel'] = 'Swiggy Instamart'
    oos_p['scraped_time'] = each_p['scraped_time']
    oos_p['city'] = each_p['city']
    oos_p['request_pincode'] = each_p['request_pincode']
    oos_p['product_id'] = each_p['product_id']
    oos_p['product_url'] = each_p['product_url']
    oos_p['brand'] = each_p['brand']

    oos.append(oos_p)

oos_data = pd.DataFrame(oos)

data = data.reindex(sorted(data.columns), axis=1)
data_id = [i + 1 for i in range(len(data))]
print(len(data), len(data_id))

data.insert(0, "Id", pd.Series(data_id))

oos_data = oos_data.reindex(sorted(oos_data.columns), axis=1)

oos_data_id = [i + 1 for i in range(len(oos_data))]

oos_data.insert(0, "Id", pd.Series(oos_data_id))

oos_data['scraped_time'] = pd.to_datetime(oos_data['scraped_time'])
oos_data['request_pincode'] = oos_data['request_pincode'].astype(int)

data['product_name'] = data['product_name'].apply(lambda x: html.unescape(x))

with pd.ExcelWriter(data_folder_loc + 'swiggy_instamart_20241002.xlsx') as writer:
    data.to_excel(writer, sheet_name='data', index=False)
    oos_data.to_excel(writer, sheet_name='out_of_stock', index=False)

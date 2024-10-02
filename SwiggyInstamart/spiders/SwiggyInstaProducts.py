import html
import json
import os.path
import random
import re
import time
from datetime import date, datetime
import pgeocode
import scrapy
import pymysql

from scrapy.cmdline import execute

from SwiggyInstamart.items import SwiggyinstamartProductItem


class SwiggyInstamartProducts(scrapy.Spider):
    name = 'InstamartProducts'

    product_IDS_M_T = {
        'MyFitness': ['K0D81W05OY', 'SP3N22IA2S', '9956KXSZ5G', '6JTI43RCFM', '7JNY31RR6T',
                      '7MVKN2CXJD', 'AAD0OM5GLH', '0P05XCZ20H', '0YSSY1LH3L', 'AFHQMG5AV3'],
        'Pebble': ['NGI4IP74FG', 'DG52GCP0B2', 'QUIPWE1KRO'],
        'Party Propz': ['YZEEFEA4W8', 'W0VCR2BFSE', 'D7E5DNYSTR', '382R5UET79']
    }

    product_IDS_O = {'MyFitness': ['K0D81W05OY', 'SP3N22IA2S', '9956KXSZ5G', '6JTI43RCFM', '7JNY31RR6T',
                                   '7MVKN2CXJD', 'AAD0OM5GLH', '0P05XCZ20H', '0YSSY1LH3L', 'AFHQMG5AV3']}

    handle_httpstatus_list = [404]

    def __init__(self):
        # Connect to MySQL database

        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='swiggy_instamart'
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):

        today_date = str(date.today()).replace('-', '_')

        folder_loc = f'C:/Shalu/PageSave/SwiggyInstamart/page_save_{today_date}/'

        if not os.path.exists(folder_loc):
            os.mkdir(folder_loc)

        pxs = [
            "185.188.76.152",
            "104.249.0.116",
            "185.207.96.76",
            "185.205.197.4",
            "185.199.117.103",
            "185.193.74.119",
            "185.188.79.150",
            "185.195.223.146",
            "181.177.78.203",
            "185.207.98.115",
            "186.179.10.253",
            "185.196.189.131",
            "185.205.199.143",
            "185.195.222.22",
            "186.179.20.88",
            "185.188.79.126",
            "185.195.213.198",
            "185.207.98.192",
            "186.179.27.166",
            "181.177.73.165",
            "181.177.64.160",
            "104.233.53.55",
            "185.205.197.152",
            "185.207.98.200",
            "67.227.124.192",
            "104.249.3.200",
            "104.239.114.248",
            "181.177.67.28",
            "185.193.74.7",
            "216.10.5.35",
            "104.233.55.126",
            "185.195.214.89",
            "216.10.1.63",
            "104.249.1.161",
            "186.179.27.91",
            "185.193.75.26",
            "185.195.220.100",
            "185.205.196.226",
            "185.195.221.9",
            "199.168.120.156",
            "181.177.69.174",
            "185.207.98.8",
            "185.195.212.240",
            "186.179.25.90",
            "199.168.121.162",
            "185.199.119.243",
            "181.177.73.168",
            "199.168.121.239",
            "185.195.214.176",
            "181.177.71.233",
            "104.233.55.230",
            "104.249.6.234",
            "104.249.3.87",
            "67.227.125.5",
            "104.249.2.53",
            "181.177.64.15",
            "104.249.7.79",
            "186.179.4.120",
            "67.227.120.39",
            "181.177.68.19",
            "186.179.12.120",
            "104.233.52.54",
            "104.239.117.252",
            "181.177.77.65",
            "185.195.223.56",
            "185.207.99.39",
            "104.249.7.103",
            "185.207.99.11",
            "186.179.3.220",
            "181.177.72.117",
            "185.205.196.180",
            "104.249.2.172",
            "185.207.98.181",
            "185.205.196.255",
            "104.239.113.239",
            "216.10.1.94",
            "181.177.77.2",
            "104.249.6.84",
            "104.239.115.50",
            "185.199.118.209",
            "104.233.55.92",
            "185.207.99.117",
            "104.233.54.71",
            "185.199.119.25",
            "181.177.78.82",
            "104.239.113.76",
            "216.10.7.90",
            "181.177.78.202",
            "104.239.119.189",
            "181.177.64.245",
            "185.199.118.216",
            "185.199.116.219",
            "185.188.77.64",
            "185.199.116.185",
            "185.188.78.176",
            "186.179.12.162",
            "185.205.197.193",
            "181.177.74.161",
            "67.227.126.121",
            "181.177.79.185",

        ]

        # for brand, product_ids in self.product_IDS_M_T.items():
        for brand, product_ids in self.product_IDS_O.items():

            if brand == "Pebble":
                excluded_city = 'Pune'
            else:
                excluded_city = 'Lucknow'

            query = f"select * from  pincode_data_new where city!='{excluded_city}' "

            self.cursor.execute(query)

            all_pincodes = self.cursor.fetchall()

            for each_product in product_ids:

                for each_pincode_tuple in all_pincodes:

                    if not os.path.exists(folder_loc + each_pincode_tuple[2] + '/'):
                        os.mkdir(folder_loc + each_pincode_tuple[2] + '/')

                    page_loc = folder_loc + each_pincode_tuple[2] + f'/{each_product}.html'
                    cookies_from_file = json.loads(
                        open(r'C:\Users\shalu.kumari\Downloads\get_cookies\swi_cookies_pincode.json', 'r').read())
                    cookies = cookies_from_file[str(each_pincode_tuple[2])]
                    cookies['strId'] = each_pincode_tuple[-2]

                    url = f"https://www.swiggy.com/instamart/item/{each_product}?storeId={each_pincode_tuple[-2]}"
                    # url='https://www.swiggy.com/instamart/item/K0D81W05OY?storeId=762613'
                    if not os.path.isfile(page_loc):

                        # continue

                        nomi = pgeocode.Nominatim('in')
                        location = nomi.query_postal_code(int(each_pincode_tuple[2]))

                        cookies1 = {
                                '__SW': 'v8lJTm8BJ3on5aXg3SbPQqQI0_0IG9_R',
                                '_device_id': 'ef532c05-f7d1-31fa-2eb3-29a6c1df65c1',
                                'fontsLoaded': '1',
                                '_gcl_au': '1.1.1253516482.1726467827',
                                'deviceId': 's%3Aef532c05-f7d1-31fa-2eb3-29a6c1df65c1.25a0iF%2FwjCxd9ra7nqTsISDj23GapDXK%2F2tauwkH6Ps',
                                'versionCode': '1200',
                                'platform': 'web',
                                'subplatform': 'dweb',
                                'statusBarHeight': '0',
                                'bottomOffset': '0',
                                'genieTrackOn': 'false',
                                'isNative': 'false',
                                'openIMHP': 'false',
                                'addressId': 's%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s',
                                'webBottomBarHeight': '0',
                                 'strId': cookies['strId'],
                                '_fbp': 'fb.1.1726467875491.625141098636543875',
                                '_gcl_aw': 'GCL.1726472353.Cj0KCQjwrp-3BhDgARIsAEWJ6SxAfFm9ei0pDR9FBMVI6nVl2iPD_ABdWSzpgdMKeTuoL9QTQMxXfu4aAlnCEALw_wcB',
                                '_gcl_gs': '2.1.k1$i1726472353',
                                # 'lat': 's%3A28.6358837.AzSink24McutgIzQ1yS2xZvhzmAYbI4olQtyDfsRU00',
                                # 'lng': 's%3A77.2208374.eGZanaOMTq%2BqgFdfVe5ldNK1kDUg2c%2Bq3sTIJMWBfVA',
                                'lat': cookies['lat'],
                                'lng': cookies['lng'],
                                # 'address': 's%3ANorthern%20Railway%2C%20NDCR%20Building%2C%20CPRO%20Office%2C%20Stat.DrqY4PO1Jg5%2BiB6fVyGXt%2Bm8J5Ykqn%2Frsz1YenjMp%2BU',
                                '_ga': 'GA1.1.2088358454.1726467827',
                                'userLocation': cookies['userLocation'],                        #     '_ga_34JYJ0BCRN': 'GS1.1.1726487598.5.1.1726487600.0.0.0',
                                'tid': 's%3A5e89a063-0d8e-454d-9702-fd2917c27ad6.cNWQSnJ3uy9BWDK5EfSWuuuJygj%2BWwsFgy%2FVoqDt3%2FY',
                                'sid': 's%3Ag69a5d94-2a2f-4a14-b271-f6e425a84b65.m2jacEhngbk6Pg%2Boky8jrilSyMUilUgB%2FK1axkXITHs',
                                'ally-on': 'false',
                                # 'strId': '',
                                'LocSrc': 's%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y',
                                'imOrderAttribution': '{%22entryId%22:null%2C%22entryName%22:null%2C%22entryContext%22:null%2C%22hpos%22:null%2C%22vpos%22:null%2C%22utm_source%22:null%2C%22utm_medium%22:null%2C%22utm_campaign%22:null}',
                                '_ga_8N8XRG907L': 'GS1.1.1726633374.8.0.1726633374.0.0.0',
                        }


                        headers = {
                            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                            'accept-language': 'en-US,en;q=0.9',
                            'cache-control': 'no-cache',
                            # 'cookie': '__SW=v8lJTm8BJ3on5aXg3SbPQqQI0_0IG9_R; _guest_tid=8e92324a-3996-4597-a624-26a303aa9a8b; _device_id=ef532c05-f7d1-31fa-2eb3-29a6c1df65c1; _sid=g4z66df5-a910-4e8a-bb5d-1cdb0d951c4b; fontsLoaded=1; _gcl_au=1.1.1253516482.1726467827; _gid=GA1.2.2059757386.1726467827; _ga_34JYJ0BCRN=GS1.1.1726467827.1.0.1726467827.0.0.0; _ga=GA1.1.2088358454.1726467827; dadl=true; deviceId=s%3Aef532c05-f7d1-31fa-2eb3-29a6c1df65c1.25a0iF%2FwjCxd9ra7nqTsISDj23GapDXK%2F2tauwkH6Ps; tid=s%3A8e92324a-3996-4597-a624-26a303aa9a8b.1drZxaz91Dueecj%2BXNJtG13v4iseveeE1CoLVVRfjYk; sid=s%3Ag4z66df5-a910-4e8a-bb5d-1cdb0d951c4b.TlBu7cnO8FUIdTM6tO24WBOJA75%2FJwg%2FsHseMD2zkV8; versionCode=1200; platform=web; subplatform=dweb; statusBarHeight=0; bottomOffset=0; genieTrackOn=false; ally-on=false; isNative=false; strId=; openIMHP=false; lat=s%3A12.9765944.fBxObcmWnJwnT%2FoZ%2BGoqGdD3T1VPilgFPtdd99CDI1A; lng=s%3A77.5992708.aSPVQXrDnNTLtpT8i9Tn23Wzvw5uz%2FX6qGw55glvbCM; address=s%3ABengaluru%2C%20Karnataka%20560001%2C%20India.tGCQDqKHP3Lz%2Bnr0zyqKYCpFVvAP43%2FBUm8UrtYdS1U; addressId=s%3A.4Wx2Am9WLolnmzVcU32g6YaFDw0QbIBFRj2nkO7P25s; LocSrc=s%3AswgyUL.Dzm1rLPIhJmB3Tl2Xs6141hVZS0ofGP7LGmLXgQOA7Y; userLocation=%7B%22address%22%3A%22Bengaluru%2C%20Karnataka%20560001%2C%20India%22%2C%22lat%22%3A12.9765944%2C%22lng%22%3A77.5992708%2C%22id%22%3A%22%22%2C%22annotation%22%3A%22%22%2C%22name%22%3A%22%22%7D; webBottomBarHeight=0; imOrderAttribution={%22entryId%22:null%2C%22entryName%22:null%2C%22entryContext%22:null%2C%22hpos%22:null%2C%22vpos%22:null%2C%22utm_source%22:null%2C%22utm_medium%22:null%2C%22utm_campaign%22:null}; _fbp=fb.1.1726467875491.625141098636543875; _ga_8N8XRG907L=GS1.1.1726467826.2.1.1726467875.0.0.0',
                            'pragma': 'no-cache',
                            'priority': 'u=0, i',
                            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"Windows"',
                            'sec-fetch-dest': 'document',
                            'sec-fetch-mode': 'navigate',
                            'sec-fetch-site': 'none',
                            'sec-fetch-user': '?1',
                            'upgrade-insecure-requests': '1',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                        }

                        time.sleep(0.5)

                        yield scrapy.Request(url, headers=headers, callback=self.parse,
                                             # cookies=random.choice([cookies_1, cookies_2]),
                                             cookies=cookies1,
                                             dont_filter=True,

                                             meta={'page_loc': page_loc,
                                                   'details_tuple': each_pincode_tuple,
                                                   'url': url,
                                                   'product_id': each_product,
                                                   "proxyy": f"http://kunal_santani577-9elgt:QyqTV6XOSp@{random.choice(pxs)}:3199",
                                                    # "proxyy":True
                                                   }
                                             )


                    else:
                        yield scrapy.Request(url="file://" + page_loc, callback=self.parse,
                                             meta={'page_loc': page_loc,
                                                   'details_tuple': each_pincode_tuple,
                                                   'url': url,
                                                   'product_id': each_product})


    def clean_name(self, value: str):
        if value.strip():
            value = (
                value.strip()
                .replace('\\', '')
                .replace('"', '\"')
                .replace("\u200c", "")
                .replace("\u200f", "")
                .replace("\u200e", "")
            )
            if "\n" in value:
                value = " ".join(value.split())
            return value


    def parse(self, response):
        # if response.status in self.handle_httpstatus_list:
        if response.status == 404:
            request = response.request
            yield request

        print(response.status)

        page_loc = response.request.meta['page_loc']
        details_tuple = response.request.meta['details_tuple']
        url = response.request.meta['url']

        if response.xpath('//script[@type= "application/ld+json"]/text()').get() is not None:

            if not os.path.isfile(page_loc):
                with open(page_loc, 'wb') as file:
                    file.write(response.body)
            print(page_loc)

            basic_data = json.loads(response.xpath('//script[@type= "application/ld+json"]/text()').get())

            items = SwiggyinstamartProductItem()

            items['store_id'] = details_tuple[-2]
            items['product_url'] = response.request.meta['url']
            items['scraped_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            items['channel'] = 'Swiggy Instamart'
            items['request_pincode'] = details_tuple[2]
            items['product_id'] = response.request.meta['product_id']
            items['store_address'] = details_tuple[3]
            items['store_fssai_license'] = details_tuple[4]
            items['store_lat_long'] = details_tuple[-1]
            items['city'] = details_tuple[1]

            items['product_name'] = basic_data['name']

            items['image_urls'] = str(' | '.join(basic_data['image']))

            items['brand'] = basic_data['brand']['name']

            items['offer_price'] = basic_data['offers']['price']

            items['price_currency'] = basic_data['offers']['priceCurrency']

            other_data = json.loads(
                response.xpath('//script[contains(text(), "window.___INITIAL_STATE__")]/text()').get().split(
                    ';  var App')[0].replace('  window.___INITIAL_STATE___ = ', ''))

            items['user_latitude'] = other_data['userLocation']['lat']
            items['user_longitude'] = other_data['userLocation']['lng']
            items['description'] = ''

            for each_widget in other_data['instamart']['cachedProductItemData']['widgetsState']:

                if each_widget['type'] == 'PRODUCT_DETAILS_WIDGET':

                    for each_it in each_widget['data'][0]['line_items']:
                        if each_it['title'] == 'Description':
                            description = each_it['description']
                            items['description'] = self.clean_name(html.unescape(re.sub(re.compile('<.*?>'), ' ',
                                                                                        description).strip()))

            other_data_section = other_data['instamart']['cachedProductItemData']['lastItemState']

            items['brand_id'] = other_data_section['brand_id']
            items['product_name_without_brand'] = other_data_section['product_name_without_brand']
            items['name_slug'] = other_data_section['name_slug']

            for product_variant in other_data_section['variations']:

                if product_variant['price']['offer_price'] == basic_data['offers']['price']:
                    items['mrp'] = product_variant['price']['mrp']

                    items['unit_price'] = product_variant['price']['unit_level_price']

                    items['applied_offer'] = product_variant['price']['offer_applied']['listing_description']

                    items['quantity'] = product_variant['quantity']

                    if items['description'] == '' or items['description'] is None:
                        try:
                            items['description'] = product_variant['meta']['long_description']
                        except:
                            items['description'] = ''

                    category_hierarchy = {
                        'l1': product_variant['super_category'],
                        'l2': product_variant['category'],
                        'l3': product_variant['sub_category_type'],
                        'l4': product_variant['sub_category']
                    }
                    items['category_hierarchy'] = json.dumps(category_hierarchy, ensure_ascii=False)

                    # items['category_hierarchy'] = str(category_hierarchy)
                    items['weight_in_grams'] = product_variant['weight_in_grams']
                    items['max_quantity'] = product_variant['max_allowed_quantity']
                    items['stock'] = product_variant['cart_allowed_quantity']['total']
                    others_dict = {
                        'dimensions': product_variant['dimensions'],
                        'disclaimer': product_variant['meta']['disclaimer'],
                        'quantity_with_combo': product_variant['sku_quantity_with_combo']
                    }

                    items['others'] = json.dumps(others_dict, ensure_ascii=True).replace(r'\u00a0', ' ')

                    if product_variant['inventory']['in_stock'] == True:

                        items['stock_status'] = 'in stock'
                    else:
                        items['stock_status'] = 'out of stock'

                    yield items
        else:
            page_loc = response.request.meta['page_loc'].replace('.html', '_not_found.html')
            if not os.path.isfile(page_loc) and response.status == 200:
                with open(page_loc, 'wb') as file:
                    file.write(response.body)
            print(page_loc)

if __name__ == '__main__':

    today_date = str(date.today()).replace('-', '_')

    folder_loc = f'C:/Shalu/LiveProjects/SwiggyInstamart/data_files/{today_date}/'

    if not os.path.exists(folder_loc):
        os.mkdir(folder_loc)

    execute(f'scrapy crawl InstamartProducts -O {folder_loc}swiggy_instamart.json'.split())




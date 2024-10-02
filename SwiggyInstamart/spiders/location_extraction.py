import os.path
import time
from typing import Iterable

import scrapy
import pandas as pd
import pgeocode
import numpy as np
from scrapy import Request
from scrapy.cmdline import execute
import requests
import json

from geopy.geocoders import Nominatim
from SwiggyInstamart.items import SwiggyinstamartLocation


class Swiggy_Locations(scrapy.Spider):

    name = 'swiggypincodes'

    def start_requests(self):

        for each_city in os.listdir('C:/Shalu/PageSave/SwiggyInstamart/pincodes'):
            print(each_city)
            for each_pincode_file in os.listdir(f'C:/Shalu/PageSave/SwiggyInstamart/pincodes/{each_city}'):
                print(each_pincode_file)

                loc = f'C:/Shalu/PageSave/SwiggyInstamart/pincodes/{each_city}/{each_pincode_file}'

                each_pincode = each_pincode_file.replace('.json', '')

                yield scrapy.Request('file://' + loc, callback=self.parse,
                                         meta={'pincode': each_pincode,
                                               'city': each_city})




    def parse(self, response):

        data = json.loads(response.body)

        print()

        items = SwiggyinstamartLocation()

        items['request_pincode'] = response.request.meta['pincode']
        items['city'] = response.request.meta['city']

        print(data.keys())

        items['store_id'] = data['data']['storeDetails']['id']
        items['store_address'] = data['data']['storeDetails']['sellerFssaiAuthorisedAddress']
        items['store_lat_long'] = data['data']['storeDetails']['lat_long']
        items['store_fssai_license'] = data['data']['storeDetails']['store_document']['fssai_license_no']
        items['store_lat_long'] = data['data']['storeDetails']['lat_long']


        yield items



if __name__ == '__main__':

    execute(f'scrapy crawl swiggypincodes'.split())

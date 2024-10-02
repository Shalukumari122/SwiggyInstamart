# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import date

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql

from SwiggyInstamart.items import SwiggyinstamartProductItem, SwiggyinstamartLocation


class SwiggyinstamartPipeline:
    def process_item(self, item, spider):
        return item


class mySQldb:

    today_date = str(date.today()).replace('-', '_')
    # print(today_date)
    def __init__(self):
        # Connect to MySQL database
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='actowiz',
            database='swiggy_instamart'
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # Define SQL query to create the table

        if isinstance(item, SwiggyinstamartProductItem):
            # item = {key: item[key] for key in sorted(item.keys())}

            try:
                query = f"""
                    CREATE TABLE IF NOT EXISTS product_data_{self.today_date} (
                    id int auto_increment primary key
                    )
                """
                # Execute the query to create the table
                self.cursor.execute(query)

                self.cursor.execute(f"SHOW COLUMNS FROM product_data_{self.today_date}")

                existing_columns = [column[0] for column in self.cursor.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name
                                in
                                item.keys()]

                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cursor.execute(f"ALTER TABLE product_data_{self.today_date} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:

                field_list = []
                value_list = []

                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')

                fields = ','.join(field_list)
                values = ", ".join(value_list)

                insert_query = f" INSERT into product_data_{self.today_date} ( " + fields + " ) values ( " + values + " )"

                self.cursor.execute(insert_query, tuple(item.values()))

                self.conn.commit()

            except Exception as e:
                print(e)



        if isinstance(item, SwiggyinstamartLocation):
            item = {key: item[key] for key in sorted(item.keys())}
            try:
                query = f"""
                    CREATE TABLE IF NOT EXISTS pincode_data_new (
                    id int auto_increment primary key
                    )
                """
                # Execute the query to create the table
                self.cursor.execute(query)

                self.cursor.execute(f"SHOW COLUMNS FROM pincode_data_new")

                existing_columns = [column[0] for column in self.cursor.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name
                                in
                                item.keys()]

                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cursor.execute(
                                f"ALTER TABLE pincode_data_new ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)

                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:

                field_list = []
                value_list = []

                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')

                fields = ','.join(field_list)
                values = ", ".join(value_list)

                insert_query = f" INSERT ignore into pincode_data_new ( " + fields + " ) values ( " + values + " )"

                self.cursor.execute(insert_query, tuple(item.values()))

                self.conn.commit()

            except Exception as e:
                print(e)

        return item

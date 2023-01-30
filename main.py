import argparse
import asyncio
import sqlite3
from datetime import date

import pandas as pd
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup, Tag
from pandas import DataFrame

from model import Parser


class ParserShop:

    def __init__(self):
        Parser.create_table()
        self.shop = ''

    @staticmethod
    async def get_all_shops(soup: BeautifulSoup):
        all_shop = soup.find_all('div', class_='shop_bg_y')
        return all_shop

    async def get_name_shop(self, shop: Tag):
        name = shop.select_one(
            "div.shop_bg_y > div.row > div.col-md-6 > a > h3")
        self.shop = name.text.split(' ')[1]

    async def iterator_positions(self, shop: Tag):
        all_position = shop.select("div.table-responsive > table > tbody > tr[class!=info]")
        for position in all_position:
            name_position = position.select_one('td.col-xs-8 > div.good-title').text.replace('\r', ''
                                                                                             ).replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '')
            count = int(position.select('td')[1].text)
            yield name_position, count

    async def create_record(self, shop: Tag):
        async for name, count in self.iterator_positions(shop):
            Parser.create(
                shop=self.shop,
                name_position=name,
                past_value_accounts=count,
            )

    async def get_all_data(self, shop: Tag):
        async for name, count in self.iterator_positions(shop):
            new_data = Parser.select().where(Parser.shop == self.shop, Parser.name_position == name,
                                             Parser.date == date.today())[:]
            if len(new_data) == 1:
                if count < new_data[0].past_value_accounts:
                    Parser.update(
                        **{"sold_count": new_data[0].sold_count + (new_data[0].past_value_accounts - count),
                           "past_value_accounts": count}
                    ).where(
                        Parser.shop == self.shop,
                        Parser.name_position == name
                    ).execute()
                elif count > new_data[0].past_value_accounts:
                    Parser.update(
                        **{"past_value_accounts": count}
                    ).where(
                        Parser.shop == self.shop,
                        Parser.name_position == name
                    ).execute()
                else:
                    continue
            elif 1 < len(new_data) < 3:
                if abs(count - new_data[0].past_value_accounts) > abs(count - new_data[1].past_value_accounts):
                    if count < new_data[1].past_value_accounts:
                        Parser.update(
                            **{"sold_count": new_data[1].sold_count + (new_data[1].past_value_accounts - count),
                               "past_value_accounts": count}
                        ).where(
                            Parser.shop == self.shop,
                            Parser.name_position == name
                        ).execute()
                    elif count > new_data[1].past_value_accounts:
                        Parser.update(
                            **{"past_value_accounts": count}
                        ).where(
                            Parser.shop == self.shop,
                            Parser.name_position == name
                        ).execute()
                elif abs(count - new_data[0].past_value_accounts) < abs(count - new_data[1].past_value_accounts):
                    if count < new_data[0].past_value_accounts:
                        Parser.update(
                            **{"sold_count": new_data[0].sold_count + (new_data[0].past_value_accounts - count),
                               "past_value_accounts": count}
                        ).where(
                            Parser.shop == self.shop,
                            Parser.name_position == name
                        ).execute()
                    elif count > new_data[0].past_value_accounts:
                        Parser.update(
                            **{"past_value_accounts": count}
                        ).where(
                            Parser.shop == self.shop,
                            Parser.name_position == name
                        ).execute()

    async def start_create(self, session):
        async with session.get(url, ssl=False) as response:
            soup = BeautifulSoup(await response.text(), "lxml")
            all_shop = await self.get_all_shops(soup)
            for shop in all_shop:
                await self.get_name_shop(shop)
                await self.create_record(shop)

    async def start(self, url, create):
        conn = TCPConnector(limit_per_host=10)
        async with ClientSession(trust_env=True, connector=conn) as session:
            if create:
                await self.start_create(session)
                return
            async with session.get(url, ssl=False) as response:
                soup = BeautifulSoup(await response.text(), "lxml")
                all_shop = await self.get_all_shops(soup)
                for shop in all_shop:
                    await self.get_name_shop(shop)
                    await self.get_all_data(shop)


def create_google_sheets(df: DataFrame):
    import gspread
    from google.oauth2.service_account import Credentials
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']

    credentials = Credentials.from_service_account_file(
        'creds.json', scopes=scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    # open a google sheet
    gs = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1A68ixW-IwAOXWtkwLDPC4eyFnWIZPq5AtKioiL2qH1k/edit#gid=0")  # select a work sheet from its name
    worksheet1 = gs.worksheet('Sheet1')
    worksheet1.clear()
    from gspread_dataframe import set_with_dataframe
    set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
                       include_column_header=True, resize=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Парсер сайта магазинов',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--create', type=str)
    args = parser.parse_args()
    if args.create.lower() == 'true':
        create = True
    elif args.create.lower() == 'false':
        create = False
    else:
        create = False
    url = 'https://lequeshop.com/goods/facebook'
    loop = asyncio.get_event_loop()
    parser = ParserShop()
    loop.run_until_complete(parser.start(url, create))
    con = sqlite3.connect("parser.db")
    data = pd.read_sql_query("SELECT shop, name_position, sold_count, date from parser", con)
    create_google_sheets(data)

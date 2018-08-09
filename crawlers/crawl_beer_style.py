import requests
from bs4 import BeautifulSoup
from lxml import html
import re
import pandas as pd
import pymysql

from mysql.config import MYSQL


def get_style_list():
    # get list
    r = requests.get('https://www.beeradvocate.com/beer/style')
    print('Request Status:', r.status_code)
    soup = BeautifulSoup(r.text, 'lxml')
    # print(soup.select("div#ba-content td[colspan='2'] a"))
    link_list = soup.select("div#ba-content td[colspan='2'] a")
    return link_list


def set_table(table_type):
    if table_type == 'style':
        df = pd.DataFrame(columns=['Style_name', 'Description'])
        df.loc[0] = ['style_name', 'description']
    return df


def db_connect(host, user, passwd, db, charset='UTF8'):
    connection = pymysql.connect(host=host,
                                 user=user,
                                 passwd=passwd,
                                 db=db,
                                 charset=charset,
                                 )
    return connection


def get_style_info(link_list):
    # get each style information
    p = re.compile('/beer/style/[\d]+/')
    df = set_table(table_type='style')
    print(df)
    i = 0
    for link in link_list:
        m = p.search(str(link))
        r_link = 'https://www.beeradvocate.com' + m.group()
        print('real link =', r_link)
        style_text = requests.get(r_link)
        soup = BeautifulSoup(style_text.text, 'lxml')
        # print(soup.select("div.titleBar h1"))
        # print(soup.select("div#ba-content span"))
        test = html.fromstring(style_text.content)
        style_name = test.xpath('//div[@class="titleBar"]/h1/text()')[0]
        style_desc = soup.br.next_sibling
        df.loc[i] = [style_name, style_desc]
        i += 1
        if i == 10:
            break
    print(df)


if __name__ == '__main__':
    link_list = get_style_list()
    get_style_info(link_list)
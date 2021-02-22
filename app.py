from tda.auth import easy_client
from tda.streaming import StreamClient

import asyncio
import json
import pickle
import sqlite3
from operator import getitem

from config import REDIRECT_URI,  ACCOUNT_NUMBER,  CLIENT_ID, DB_FILE


# primary gappers by ticker using td info
all_dictionary = {}

# list of finviz tickers from db
ticker_list_primary = None

connection = sqlite3.connect(DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("SELECT symbol from stock")
rows = cursor.fetchall()
ticker_list_primary = [row['symbol'] for row in rows]
# print(str(ticker_list_primary))




client = easy_client(
        api_key= CLIENT_ID,
        redirect_uri= REDIRECT_URI,
        token_path='/tmp/token.pickle')
stream_client = StreamClient(client, account_id=ACCOUNT_NUMBER)

async def get_primary_gappers(content):
    global all_dictionary
    
    try:
        for asset in content:
            ticker = asset['key']
            if ticker not in all_dictionary:
                all_dictionary[ticker] = {
                    'bid': asset['1'],
                    'ask': asset['2'],
                    'last': asset['3'],
                    'total_volume': asset['8'],
                    'close': asset['15'],
                    'description': asset['25'],
                    'gap': float((asset['3']-asset['15']) / asset['15'])
                }
            elif ('3' in asset) and ('close' in all_dictionary[ticker]):
                # recalc the gap %
                all_dictionary[ticker]['gap'] = float((asset['3']-all_dictionary[ticker]['close']) / all_dictionary[ticker]['close'])

                # go through the other keys
                for k in asset:
                    identities = {
                        '1': 'bid',
                        '2':'ask',
                        '3':'last',
                        '8':'total_volume',
                        '15':'close',
                        '25':'description',
                    }
                    if k in identities:
                        id = identities[k]
                        all_dictionary[ticker][id] = asset[k]
            elif ('3' in asset) and ('15' in asset):
                # recalc the gap %
                all_dictionary[ticker]['gap'] = float((asset['3']-asset['15']) / asset['15'])

                # go through the other keys
                for k in asset:
                    identities = {
                        '1': 'bid',
                        '2':'ask',
                        '3':'last',
                        '8':'total_volume',
                        '15':'close',
                        '25':'description',
                    }
                    if k in identities:
                        id = identities[k]
                        all_dictionary[ticker][id] = asset[k]
            else:
                # go through the other keys
                for k in asset:
                    identities = {
                        '1': 'bid',
                        '2':'ask',
                        '3':'last',
                        '8':'total_volume',
                        '15':'close',
                        '25':'description',
                    }
                    if k in identities:
                        id = identities[k]
                        all_dictionary[ticker][id] = asset[k]


    except Exception as e:
        with open('ERROR_LOG.txt', 'a+', buffering=1) as file:
            l = ['\n\n **** New Error: ***** {}'.format(e), '\n{}'.format(asset)]
            file.writelines(l)
    
    finally:
        # write 15 gappers to file
        s = sorted(all_dictionary.items(), reverse=True, key=lambda x: getitem(x[1], 'gap'))[:15]
        t = ['\n{}     {:.2%}'.format(x[0], x[1]['gap']) for x in s]
        with open('PRIMARY_GAPPERS.txt', 'a+', buffering=1) as file:
            file.write("\n\n\n*************************")
            file.writelines(t)
        return 'Done'

async def read_stream():
    await stream_client.login()
    await stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)

    # Always add handlers before subscribing because many streams start sending
    # data immediately after success, and messages with no handlers are dropped.
    stream_client.add_level_one_equity_handler(get_primary_gappers)


    await stream_client.level_one_equity_subs(ticker_list_primary, fields="0,1,2,3,8,15,25")

    while True:
        await stream_client.handle_message()

asyncio.run(read_stream())
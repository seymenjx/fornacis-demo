from aiohttp import http
from requests import get
import pandas as pd
from web3 import Web3, HTTPProvider
import sqlite3
from time import sleep

CONTRACTADDRESS = " " #Address of NFT contract
COLUMNS = ['transaction_hash', 'transaction_index', 'token_ids', 'seller_address',
       'buyer_address', 'token_address', 'marketplace_address', 'price',
       'price_token_address', 'block_timestamp', 'block_number', 'block_hash', 'stage_conditions', 'ticket_minted', 'nft_sended', 'streak_setted']

provider = Web3(HTTPProvider("")) #web3 endpoint
conn = sqlite3.connect('transactions.sql')
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS transactions (transaction_hash text, transaction_index text, token_ids text, seller_address text, buyer_address text, token_address text, marketplace_address text, price number,price_token_address text, block_timestamp text, block_number number, block_hash text, stage_conditons number, ticket_minted text, nft_sended text, streak_setted text)')
conn.commit()


def firststart(contractaddress, con, provider):

    global alltransaction
    url = f"https://deep-index.moralis.io/api/v2/nft/{contractaddress}/trades?chain=eth&to_block=16514563&marketplace=opensea&limit=10000000"#block= provider.eth.blockNumber - 150

    headers = {
        "accept": "application/json",
        "X-API-Key": "zmr7CqkKsvHBmQgI4JZ8L6SEpqy67RuWSi8Bd9fwQL2PlE7qNNyBqP8JfUAmticZ"
    }

    response = get(url, headers=headers)
    alltransaction = pd.DataFrame(response.json()["result"])
    alltransaction["transaction_hash"] = alltransaction["transaction_hash"].astype("str")
    alltransaction["transaction_index"] = alltransaction["transaction_index"].astype("str")
    alltransaction["token_ids"] = alltransaction["token_ids"].astype("str")
    alltransaction["seller_address"] = alltransaction["seller_address"].astype("str")
    alltransaction["buyer_address"] = alltransaction["buyer_address"].astype("str")
    alltransaction["token_address"] = alltransaction["token_address"].astype("str")
    alltransaction["marketplace_address"] = alltransaction["marketplace_address"].astype("str")
    alltransaction["price"] = alltransaction["price"].astype("float64")
    alltransaction["price_token_address"] = alltransaction["price_token_address"].astype("str")
    alltransaction["block_timestamp"] = alltransaction["block_timestamp"].astype("str")
    alltransaction["block_number"] = alltransaction["block_number"].astype("float64")
    alltransaction["block_hash"] = alltransaction["block_hash"].astype("str")
    alltransaction.insert(0, column="stage_conditions", value=0)
    alltransaction.insert(0, column="ticket_minted", value= None)
    alltransaction.insert(0, column="nft_sended", value=None)
    alltransaction.insert(0, column="streak_setted", value="nan")

    alltransaction.to_sql('transactions', con, if_exists='replace', index = False)

def main(contractaddress, con, columns, provider):
    
    sql_query = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM transactions
                               ''', con)
    
    alltransaction = pd.DataFrame(sql_query, columns = columns)

    url = f"https://deep-index.moralis.io/api/v2/nft/{contractaddress}/trades?chain=eth&from_block={alltransaction['block_number'][0]+1}&to_block={provider.eth.blockNumber - 150}&marketplace=opensea&limit=10000000"

 

    headers = {
        "accept": "application/json",
        "X-API-Key": " " #moralis apikey
    }

    response = get(url, headers=headers)
    df1 = pd.DataFrame(response.json()["result"])
    df1.insert(0, column="stage_conditions", value=0)
    df1.insert(0, column='ticket_minted', value= None)
    df1.insert(0, column='nft_sended', value= "nan")
    alltransaction = pd.concat([df1, alltransaction.loc[:]]).reset_index(drop=True)#adds new txs to the top of dataframe
    

    alltransaction["transaction_hash"] = alltransaction["transaction_hash"].astype("str")
    alltransaction["transaction_index"] = alltransaction["transaction_index"].astype("str")
    alltransaction["token_ids"] = alltransaction["token_ids"].astype("str")
    alltransaction["seller_address"] = alltransaction["seller_address"].astype("str")
    alltransaction["buyer_address"] = alltransaction["buyer_address"].astype("str")
    alltransaction["token_address"] = alltransaction["token_address"].astype("str")
    alltransaction["marketplace_address"] = alltransaction["marketplace_address"].astype("str")
    alltransaction["price"] = alltransaction["price"].astype("float64")
    alltransaction["price_token_address"] = alltransaction["price_token_address"].astype("str")
    alltransaction["block_timestamp"] = alltransaction["block_timestamp"].astype("str")
    alltransaction["block_number"] = alltransaction["block_number"].astype("float64")
    alltransaction["block_hash"] = alltransaction["block_hash"].astype("str")
    alltransaction['stage_conditions'] = alltransaction['stage_conditions'].astype("int")
    alltransaction['ticket_minted'] = alltransaction['ticket_minted'].astype("str")
    alltransaction['nft_sended'] = alltransaction['nft_sended'].astype("str")
    alltransaction['streak_setted'] = alltransaction['streak_setted'].astype("str")

    
    alltransaction.to_sql('transactions', conn, if_exists='replace', index = False)

    print(alltransaction['block_number'][0])
    print(alltransaction.shape)
    print(alltransaction['stage_conditions'][0])

firststart(contractaddress = Web3.toChecksumAddress(CONTRACTADDRESS), con=conn, provider=provider)

n = 0
while n<10:
    main(contractaddress =Web3.toChecksumAddress(CONTRACTADDRESS), con=conn, columns=COLUMNS, provider=provider)
    n+=1 
    sleep(120)#maybe 15 minutes for preventing possible data losing
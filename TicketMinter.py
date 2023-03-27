import sqlite3
from web3 import Web3
from requests import post
import pandas as pd
from moralis import evm_api
import json
import time





class Streak:


    api_key = "" #moralis api key
    colectionAddress = '' #address of NFT contract
    
    columns = ['transaction_hash', 'transaction_index', 'token_ids', 'seller_address',
        'buyer_address', 'token_address', 'marketplace_address', 'price',
        'price_token_address', 'block_timestamp', 'block_number', 'block_hash', 'stage_conditions', 'ticket_minted', 'nft_sended', 'streak_setted']

    oldShape =  0
    def createdb(self):
        self.conn = sqlite3.connect('transactions.sql')
        sql_query = pd.read_sql_query ('''
                                        SELECT
                                        *
                                        FROM transactions
                                        ''', self.conn)

        


        self.transactions = pd.DataFrame(sql_query, columns = self.columns)





    def checkValidnes(self): 

        for index, row in self.transactions.iterrows():
            if row['ticket_minted'] != None and row['streak_setted'] == None:


            
                    pass
        

    def increaseStreak(self, tokenIDs):
        for id in tokenIDs:
            if id<3000:
                file = open(f"Untitled Project/metadata/{id}")
                data = json.load(file)
                file.close()
                data['attributes'][6]["value"] = data['attributes'][6]["value"] + 1
                fileW = open(f"Untitled Project/metadata/{id}", "w")
                jsonString = json.dumps(data)
                fileW.write(jsonString)
                fileW.close()
                print(f'{tokenIDs} increased 1')
            else:
                print("upsi")

    def getNfts(self, wallet):

        wallet = Web3.toChecksumAddress(wallet)

        params = {
            "address": wallet, 
            "chain": "eth", 
            "format": "decimal", 
            "limit": 10, 
            "token_addresses": [Web3.toChecksumAddress(self.colectionAddress)], 
            "cursor": "", 
            "normalizeMetadata": True, 
        }

        result = evm_api.nft.get_wallet_nfts(
            api_key=self.api_key,
            params=params,
        )

        if result['result'] == []:
            print(f'{wallet} has none collectible')
            return []

        else:
            df1 = pd.DataFrame(result["result"])
            print(f'''{wallet} has collectible and they are: {df1['token_id'].astype("int").to_list()}''')
            return df1['token_id'].astype("int").to_list()
    


    def addToDB(self, index, row, tokenIds):
        self.transactions.loc[index, 'streak_setted'] = tokenIds
        self.transactions["transaction_hash"] =self.transactions["transaction_hash"].astype("str")
        self.transactions["transaction_index"] = self.transactions["transaction_index"].astype("str")
        self.transactions["token_ids"] = self.transactions["token_ids"].astype("str")
        self.transactions["seller_address"] = self.transactions["seller_address"].astype("str")
        self.transactions["buyer_address"] = self.transactions["buyer_address"].astype("str")
        self.transactions["token_address"] = self.transactions["token_address"].astype("str")
        self.transactions["marketplace_address"] = self.transactions["marketplace_address"].astype("str")
        self.transactions["price"] = self.transactions["price"].astype("float64")
        self.transactions["price_token_address"] = self.transactions["price_token_address"].astype("str")
        self.transactions["block_timestamp"] = self.transactions["block_timestamp"].astype("str")
        self.transactions["block_number"] = self.transactions["block_number"].astype("float64")
        self.transactions["block_hash"] = self.transactions["block_hash"].astype("str")
        self.transactions['stage_conditions'] = self.transactions['stage_conditions'].astype("int")
        self.transactions['ticket_minted'] = self.transactions['ticket_minted'].astype("str")
        self.transactions['nft_sended'] =self.transactions['nft_sended'].astype("str")
        self.transactions['streak_setted'] = self.transactions['streak_setted'].astype("str")

        self.transactions.to_sql('transactions', self.conn, if_exists='replace', index = False)


    def client(self):    
        while True:
            named_tuple = time.localtime() # gets struct_time
            time_string = time.strftime("%H:%M", named_tuple)
            if True or time_string == '17:27':
                self.createdb()
                for index, row in self.transactions.iterrows():
                    #print(row['seller_address'])
                    if row['stage_conditions'] == 1  and row['streak_setted'] == "nan":
                        tokenIds = self.getNfts(row['seller_address']) #should be seller_address but i changed it to buyyer_address for test do not forget to fix 
                        print("tokenids", tokenIds)
                        if tokenIds == []:
                            continue    
                        else:    
                            self.increaseStreak(tokenIds)
                            self.addToDB(index=index,row=row,tokenIds=f"{tokenIds}")



            else:
                pass




if __name__ == "__main__":
    mechanism = Streak()
    mechanism.client()

'''

DEMO SCRIPT

'''

import pandas as pd
import json
import sqlite3
from multiprocessing import process
from requests import get, post
from web3 import Web3, HTTPProvider
import asyncio
from web3.auto import w3
from time import sleep
SEAPORT = '0x00000000006c3852cbef3e08e8df289169ede581'


class Mechanism:
    stage1Limit = 0.02
    stage2Limit = 0.035
    stage3Limit = 0.07
    stage4Limit = 1
    stage5Limit = 10

    stage1online = True
    stage2online = False
    stage3online = False
    stage4online = False
    stage5online = False

    remainStage1supply = 100
    remainStage2supply = 100
    remainStage3supply = 100
    remainStage4supply = 100
    remainStage5supply = 100


    luckyNumbers = [4, 5, 15, 29, 31]

    provider = Web3(HTTPProvider("")) #Web3 endpoint 
    ticketContractadr = Web3.toChecksumAddress("") #ticket contract address
    f = open("abi.json", "r")
    abi = json.load(f)
    ticketContract = provider.eth.contract(address=ticketContractadr, abi=abi)
    chainID = 5 #1 for mainnet 5 for goerli
    chainname = "goerli"
    ticketIndex = 0 #find a solution to this
    ticketBlock = 7776088

    account = {"public": " ",
            "private": " "} #wallet

    account_zero = "0x0000000000000000000000000000000000000000"

    columns = ['transaction_hash', 'transaction_index', 'token_ids', 'seller_address',
       'buyer_address', 'token_address', 'marketplace_address', 'price',
       'price_token_address', 'block_timestamp', 'block_number', 'block_hash', 'stage_conditions', 'ticket_minted', 'nft_sended', 'streak_setted']

    conn = sqlite3.connect('transactions.sql') 
          
    sql_query = pd.read_sql_query ('''
                                   SELECT
                                   *
                                   FROM transactions
                                   ''', conn)
    
    df = pd.DataFrame(sql_query, columns = columns)
    whereWeLeft = 0

    def client(self): #what if dataframe's rows run out?
        while True:
            if self.stage1online:
                for index, row in self.df.iterrows():
                    self.setSql()
                    if index <= self.whereWeLeft:
                        continue
                    else:
                       self.stage1(row, index)
            
            elif self.stage2online:
                for index, row in self.df.iterrows():
                    self.setSql()
                    if index <= self.whereWeLeft:
                        continue
                    else:
                        self.stage2(row, index)
            
            elif self.stage3online:
                for index, row in self.df.iterrows():
                    self.setSql()
                    if index <= self.whereWeLeft:
                        continue
                    else:
                        self.stage3(row, index)
            
            elif self.stage4online:
                for index, row in self.df.iterrows():
                    self.setSql()
                    if index <= self.whereWeLeft:
                        continue
                    else:
                        self.stage4(row, index)
            
            elif self.stage5online:
                for index, row in self.df.iterrows():
                    self.setSql()
                    if index <= self.whereWeLeft:
                        continue
                    else:
                        self.stage5(row, index)
            else:
                print("All done we are rich now!")


    def stage1(self, row, index):
    
        if self.remainStage1supply > 0:
            if row["price"]/10**18 >= self.stage1Limit:
                self.setIndex()
                data = self.mintTicket(Web3.toChecksumAddress(row["buyer_address"]))
                self.ticketIdToDB(index=index, ticketId=data["ticketId"], txHash=data["hash"])
                self.setStageConditionsSql(index)
                if self.ticketIndex in self.luckyNumbers:
                    self.sendNFT(row["buyer_address"])
                    self.remainStage1supply -= 1
                else:
                    pass
            else:
                pass

        else:
            self.stage1online = False
            self.stage2online = True
            print("stage 2 live") #create massage to blockchain

    def stage2(self, row, index):
        if self.remainStage2supply > 0:
            if row["price"]/10**18 >= self.stage2Limit:
                self.setIndex()
                data = self.mintTicket(Web3.toChecksumAddress(row["buyer_address"]))
                self.ticketIdToDB(index=index, ticketId=data["ticketId"], txHash=data["hash"])
                self.setStageConditionsSql(index)
                if self.ticketIndex in self.luckyNumbers:
                    self.sendNFT(row["buyer_address"])
                    self.remainStage2supply -= 1
                else:
                    pass
            else:
                pass

        else:
            self.stage2online = False
            self.stage3online = True
            print("stage 3 live") #create massage to blockchain


    def stage3(self, row, index):
        if self.remainStage3supply > 0:
            if row["price"]/10**18 >= self.stage3Limit:
                self.setIndex()
                data = self.mintTicket(Web3.toChecksumAddress(row["buyer_address"]))
                self.ticketIdToDB(index=index, ticketId=data["ticketId"], txHash=data["hash"])
                self.setStageConditionsSql(index)
                if self.ticketIndex in self.luckyNumbers:
                    self.sendNFT(row["buyer_address"])
                    self.remainStage3supply -= 1
                else:
                    pass
            else:
                pass

        else:
            self.stage3online = False
            self.stage4online = True
            print("stage 4 live") #create massage to blockchain


    def stage4(self, row, index): 
        if self.remainStage4supply > 0:
            if row["price"]/10**18 >= self.stage4Limit:
                self.setIndex()
                data = self.mintTicket(Web3.toChecksumAddress(row["buyer_address"]))
                self.ticketIdToDB(index=index, ticketId=data["ticketId"], txHash=data["hash"])
                self.setStageConditionsSql(index)
                if self.ticketIndex in self.luckyNumbers:
                    self.sendNFT(row["buyer_address"])
                    self.remainStage4supply -= 1
                else:
                    pass
            else:
                pass

        else:
            self.stage4online = False
            self.stage5online = True
            print("stage 5 live") #create massage to blockchain


    def stage5(self, row, index):
        if self.remainStage5supply > 0:
            if row["price"]/10**18 >= self.stage5Limit:
                self.setIndex()
                data = self.mintTicket(Web3.toChecksumAddress(row["buyer_address"]))
                self.ticketIdToDB(index=index, ticketId=data["ticketId"], txHash=data["hash"])
                self.setStageConditionsSql(index)
                if self.ticketIndex in self.luckyNumbers:
                    self.sendNFT(row["buyer_address"])
                    self.remainStage5supply -= 1
                else:
                    pass
            else:
                pass

        else:
            self.stage5online = False
            print("None left!") #create a massage to blockchain



    def mintTicket(self, to):
        unstx = self.ticketContract.functions.safeMint(to, int(self.ticketIndex)).buildTransaction(
            {"chainId" : self.chainID, 
            "from": self.account["public"],
            'gasPrice': self.provider.eth.gas_price, "nonce": self.provider.eth.get_transaction_count(self.account["public"])})
        sntx = w3.eth.account.sign_transaction(unstx, self.account["private"])
        txhash = self.provider.eth.send_raw_transaction(sntx.rawTransaction)
        tx_reciept = self.provider.eth.wait_for_transaction_receipt(txhash)
        if tx_reciept['status'] == 1:
            print(self.ticketIndex)
            print(self.df.shape)
            sleep(15)
            return {"hash" : tx_reciept["transactionHash"].hex(), 
                    "gasUsed": tx_reciept["gasUsed"], 
                    "to": to,
                    "block": tx_reciept["blockNumber"],
                    "ticketId" : self.ticketIndex}

        else:
            pass

    def setIndex(self): 
    
        headers = {
            'accept': 'application/json',
            'X-API-Key': ' ', #moralis apikey
            # Already added when you pass json= but not when you pass data=
            # 'Content-Type': 'application/json',
        }

        params = {
            'chain': f'0x{self.chainID}',
            'from_block': f'{self.ticketBlock}',
            'topic': 'Transfer(address,address,uint256)',
        }

        json_data = {
            'anonymous': False,
            'inputs': [
                {
                    'indexed': True,
                    'internalType': 'address',
                    'name': 'from',
                    'type': 'address',
                },
                {
                    'indexed': True,
                    'internalType': 'address',
                    'name': 'to',
                    'type': 'address',
                },
                {
                    'indexed': True,
                    'internalType': 'uint256',
                    'name': 'tokenId',
                    'type': 'uint256',
                },
            ],
            'name': 'Transfer',
            'type': 'event',
        }

        respo = post(f'https://deep-index.moralis.io/api/v2/{self.ticketContractadr}/events', params=params, headers=headers, json=json_data)

        li = []
        for result in respo.json()["result"]:
            #print(result["data"])
            li.append(result["data"])


        dfmi = pd.DataFrame(li)
        #dfmi = dfmi.drop(columns=["Unnamed: 0"])
        dfres = pd.DataFrame(respo.json()["result"])
        dfres = dfres.drop(columns=["data"])  #        dfres = dfres.drop(columns=["data", "Unnamed: 0"])

        frames = [dfres ,dfmi]
        finaldf = pd.concat(frames,axis=1) 
        minted = finaldf[finaldf['from'] == self.account_zero ]
        self.ticketIndex = minted["tokenId"].astype(int).max() + 1
        self.ticketBlock = minted["block_number"].max()
    

    def sendNFT(self, to): #dont forget to update supply. Inside in this function or general
        print('nft sended')

    def setRandomNumbers(self):
        pass

    def setSql(self):

    
        self.df = pd.DataFrame(self.sql_query, columns = self.columns)

    def setStageConditionsSql(self, index):
        
        self.df.loc[index, ['stage_conditions']] = 1
        
        self.df["transaction_hash"] = self.df["transaction_hash"].astype("str")
        self.df["transaction_index"] = self.df["transaction_index"].astype("str")
        self.df["token_ids"] = self.df["token_ids"].astype("str")
        self.df["seller_address"] = self.df["seller_address"].astype("str")
        self.df["buyer_address"] = self.df["buyer_address"].astype("str")
        self.df["token_address"] = self.df["token_address"].astype("str")
        self.df["marketplace_address"] = self.df["marketplace_address"].astype("str")
        self.df["price"] = self.df["price"].astype("float64")
        self.df["price_token_address"] = self.df["price_token_address"].astype("str")
        self.df["block_timestamp"] = self.df["block_timestamp"].astype("str")
        self.df["block_number"] = self.df["block_number"].astype("float64")
        self.df["block_hash"] = self.df["block_hash"].astype("str")
        self.df['stage_conditions'] = self.df['stage_conditions'].astype("int")
        self.df['ticket_minted'] = self.df['ticket_minted'].astype("str")
        self.df['nft_sended'] = self.df['nft_sended'].astype("str")
        self.df['streak_setted'] = self.df['streak_setted'].astype("str")

        self.df.to_sql('transactions', self.conn, if_exists='replace', index = False) #if exists replace????? 

    def ticketIdToDB(self, index, ticketId, txHash):

        self.df.loc[index, 'ticket_minted'] = f"{ticketId},{txHash}" 



if __name__ == "__main__":
    mechanism = Mechanism()
    mechanism.client()

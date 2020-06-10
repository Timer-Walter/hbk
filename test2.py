import json
import pandas as pd
import time
import talib
from HuobiDMService import HuobiDM


def Buy(closed,opened,amounted,ma5,ma60,posFirst):

    if ma5[-1] < ma60[-1] and ma5[-2] > ma60[-2]:
        angle = ((ma5[-2] - ma5[-1]) / ma5[-2] + (ma5[-3] - ma5[-2]) / ma5[-3]) * 10000
        if angle>3 and closed[-1]<opened[-1]:
            amountDown = amounted[-1] / float(amounted[-2])
            a = [ma60[-2], ma60[-4], ma60[-6]]
            if amountDown>2 and a == sorted(a):
                return posFirst

    return 0

def Sell(closed,opened,amounted,ma5,ma20,ma30):

    if (ma5[-1] > ma30[-1] and ma5[-2] <= ma30[-2]) or (ma5[-1] > ma20[-1] and ma5[-2] <= ma20[-2]):
        if closed[-1]>opened[-1]:
            amountUp = amounted[-1] / float(amounted[-2])
            if amountUp>5:
                return 1

    return 0

# account_info = {'margin_available':1000,'margin_frozen':0,'volume':0,'price':0,'cost_price':0,'id':0}
#
# with open("test1_account_info.json", "w") as dump_f:
#     json.dump(account_info, dump_f)
testList=["test1_account_info.json","test2_account_info.json","test3_account_info.json","test4_account_info.json"]

account_info_list=[]
params=[[0.5,10],[0.5,20],[1,10],[1,20]]
for t in testList:
    with open(t, 'r') as load_f:
        account_info = json.load(load_f)
        account_info_list.append(account_info)

URL = 'https://www.hbdm.com/'
ACCESS_KEY = 'h6n2d4f5gh-2807e0ad-bf09aa85-4d293'
SECRET_KEY = '42d2b281-7223ee07-a0c3fe43-d52ca'
count = 0
retryCount =0
while (1):
    try:
        dm = HuobiDM(URL, ACCESS_KEY, SECRET_KEY)
        kline_1min = (dm.get_contract_kline(symbol='BTC_CQ', period='1min'))['data']
    except:
        retryCount += 1
        if(retryCount == 20):
            for i in range(4):
                with open(testList[i], "w") as dump_f:
                    json.dump(account_info_list[i], dump_f)
            print('connect ws error!')
            break
        continue

    retryCount=0

    kline = (pd.DataFrame.from_dict(kline_1min))[['id', 'close', 'high', 'low', 'open', 'amount']]
    id = kline['id'].values
    id = (id[-1] / 60)
    closed = kline['close'].values
    opened = kline['open'].values
    highed = kline['high'].values
    lowed = kline['low'].values
    amounted = kline['amount'].values
    ma5 = talib.SMA(closed, timeperiod=5)
    ma20 = talib.SMA(closed, timeperiod=20)
    ma30 = talib.SMA(closed, timeperiod=30)
    ma60 = talib.SMA(closed, timeperiod=60)

    for i,account_info in enumerate(account_info_list):
        if (account_info['margin_available'] + account_info['margin_frozen'] +(account_info['price'] - highed[-1]) * account_info['volume']) <= 0:
            account_info['margin_available'] = 0
            account_info['margin_frozen'] = 0
            account_info['cost_price'] = 0

        account_info['margin_available'] += (account_info['price'] - closed[-1]) * account_info['volume']
        account_info['price'] = closed[-1]


        if(id!=account_info['id']):
            position = max(0, Buy(closed, opened, amounted, ma5, ma60,params[i][0]))
            price = closed[-1] * (1 - 0.001)
            if id - account_info['id'] == 1 and closed[-1] < opened[-1] and 6 > amounted[-1] / float(amounted[-2]) > 2:
                position = max(position, 1)

            if position > 0 and account_info['margin_available'] > 0:
                margin_available_use = account_info['margin_available'] * position * params[i][1] * (1 - 0.0003)
                volume_add = margin_available_use / price
                account_info['volume'] += volume_add
                account_info['margin_frozen'] += account_info['margin_available'] * position
                account_info['margin_available'] -= account_info['margin_available'] * position
                account_info['cost_price'] = closed[-1]
                account_info['id'] = id

        if account_info['cost_price'] != 0:
            position=0
            if closed[-1] > account_info['cost_price'] * 1.01 or closed[-1] < account_info['cost_price'] * 0.96:
                position = 1
            else:
                position = Sell(closed, opened, amounted, ma5, ma20, ma30)

            if position > 0:
                account_info['margin_available'] += account_info['margin_frozen'] * position
                account_info['margin_frozen'] -= account_info['margin_frozen'] * position
                account_info['margin_available'] -= account_info['volume'] * position * closed[-1] * 0.001
                account_info['volume'] -= account_info['volume'] * position
                if position == 1:
                    account_info['cost_price'] = 0
                    account_info['id'] = 0
    count +=1

    if(count%20==0):
        s=[]
        for i in range(4):
            s.append(account_info_list[i]['margin_available'] + account_info_list[i]['margin_frozen'])
            with open(testList[i], "w") as dump_f:
                json.dump(account_info_list[i], dump_f)
        print(s)

    time.sleep(10)

import websocket
import os

from utils import Poloniex

from multiprocessing.dummy import Process as Thread
#import pymongo
import redis
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DictTicker(object):

    def __init__(self, api=None):
        self.time_key = 'MINS_SPENT'
        self.redis_name_hash = 'NAME_HASH'
        self.main_hash = 'MARKET_HASH'
        key = os.environ['POLO_KEY']
        secret = os.environ['POLO_SECRET']
        host = os.environ['REDIS_HOST']
        port = os.environ['REDIS_PORT']
        self.db = redis.Redis(host=host, port=port)
        #self.db = pymongo.MongoClient().poloniex['ticker']
        #self.db.drop()
        #self.api = api
        #if not self.api:
        self.api = Poloniex(key, secret)
        self.tick = {}
        self.populateTicker()
        self.start_time = datetime.now()

        #iniTick = self.api.returnTicker()
        #self._ids = {market: iniTick[market]['id'] for market in iniTick}
        #for market in iniTick:
        #    self.tick[self._ids[market]] = iniTick[market]

        self._ws = websocket.WebSocketApp(
            "wss://api2.poloniex.com/",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close)

    def populateTicker(self):
        initTick = self.api.returnTicker()
        self.start_time = datetime.now()
        self.tick = {}
        for market, data in initTick.items():
            logger.info(market)
            logger.info(data)
            item = {}

            _id = int(data['id'])
            item["market"] = market
            bid = float(data['highestBid'])
            ask = float(data['lowestAsk'])
            item["initValue"] = ask
            item["currValue"] = bid
            percent = 100. * (bid - ask) / ask
            item["percent"] = '{:.2f}'.format(percent)
            item['up'] = 0
            item['down'] = 0
            self.db.hset(self.main_hash, _id, json.dumps(item))
            self.db.hset(self.redis_name_hash, market, _id)
            self.tick.update({_id: ask})
            #self.db.update_one(
            #    {'_id': market},
            #    {'$set': item})
        logger.info('populated markets with initial data')

    def on_message(self, ws, message):
        info = json.loads(message)
        logger.error(info)
        if info[0] == 1002:
            # get num minutes since start
            mins = (datetime.now() - self.start_time).total_seconds() / 60
            logger.info('{} mins taken'.format(mins))

            data = [float(i) for i in info[2]]
            _id = int(info[2][0])
            _item = self.db.hget(self.main_hash, _id)
            #item = self.db.find_one({'_id': id})
            if not _item:
                logger.exception('could not find item with id {}'.format(id))
            else:
                #logger.info(_item)
                item = json.loads(_item.decode('utf-8'))
                ask = self.tick.get(_id)
                bid = item['currValue']
                curr_val = data[3]
                if curr_val > item['currValue']:
                    item['up'] = item['up'] + 1
                elif curr_val < item['currValue']:
                    item['down'] = item['down'] + 1
                #init_val = float(item['initValue'])
                percent = 100. * (curr_val - ask) / ask
                #logger.error('ask is {}, bid is {}, percent is {}'.format(ask, curr_val, percent))
                item['percent'] = '{:.2f}'.format(percent)
                item['currValue'] = curr_val
                self.db.hset(self.main_hash, id, json.dumps(item))
                self.db.set(self.time_key, mins)
                #self.db.bgsave()
                #self.db.update_one(
                #    {'_id': market},
                #    {'$set': {
                #        'currValue': curr_val,
                #        'percent': '{:.2f}'.format(percent)
                #        }
                #    })
        #logger.info('item saved')

    def on_error(self, ws, error):
        logger.error(error)

    def on_close(self, ws):
        if self._t._running:
            try:
                self.stop()
            except Exception as e:
                logger.exception(e)
            try:
                self.start()
            except Exception as e:
                logger.exception(e)
                self.stop()
        else:
            logger.info('websocket closed')

    def on_open(self, ws):
        self._ws.send(json.dumps({'command': 'subscribe', 'channel': 1002}))

    @property
    def status(self):
        try:
            return self._t._running
        except:
            return False

    def start(self):
        #self.start_time = datetime.now()
        #self.populateTicker()
        self._t = Thread(target=self._ws.run_forever)
        self._t.daemon = True
        self._t._running = True
        self._t.start()
        logger.info('websocket started')

    def stop(self):
        self._t._running = False
        self._ws.close()
        self._t.join()
        logger.info('websocket stopped')

    def __call__(self, market='BTC_LSK'):
        id = self.db.hget(self.redis_name_hash, market)
        return self.db.hget(self.main_hash, id)


if __name__ == '__main__':
    #import pprint
    from time import sleep
    logging.basicConfig(level=logging.DEBUG)
    ticker = DictTicker()

    try:
        ticker.start()
        while True:
            sleep(900)
            ticker.stop()
            ticker.populateTicker()
            sleep(2)
            ticker.start()
        #for i in range(5):
        #    sleep(60)
        #    print(i)
    except Exception as e:
        logger.exception(e)
    #ticker.stop()

#!/usr/bin/python3

import os
import sys
import logging
import tempfile
import requests
import traceback
import argparse
from multiprocessing import Pool

import time
import datetime
import schedule

import pandas as pd
import pandas_ta as ta
import investpy
import mplfinance as mpf

# ロギング
logger = logging.getLogger(__name__)

# パーサー
parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='print debug log')
parser.add_argument('--single', action='store_true', help='single process')
parser.add_argument('--run', action='store_true', help='not schedule')
parser.add_argument('-t', '--target', default='', help='symbol')
args = parser.parse_args()

def setup_logger():
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)s %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    path = os.path.dirname(os.path.abspath(__file__)) + '/trace.log'
    file_handler = logging.FileHandler(filename = path)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger


def call_with_retry(func, *args, **kwargs):
    retry_max = 20

    for i in range(retry_max):
        try:
            return func(*args, **kwargs)
        except ConnectionError as e:
            if i == retry_max - 1:
                raise e
            else:
                logger.warning(' -- {} retry {}/{}'.format(func.__name__, i + 1, retry_max))
                time.sleep(1)


def get_histrical_data(symbol):
    try:
        # 期間
        end   = datetime.date.today()
        start = datetime.date.today() - datetime.timedelta(days = 200)
        end   = datetime.datetime.strftime(end,   '%d/%m/%Y')
        start = datetime.datetime.strftime(start, '%d/%m/%Y')

        return call_with_retry(
            investpy.stocks.get_stock_historical_data,
            stock = symbol,
            country = 'japan',
            from_date = start,
            to_date = end,
        )
    except:
        etype, evalue, _ = sys.exc_info()
        execption = traceback.format_exception_only(etype, evalue)[0].rstrip('\r\n')
        logger.error(' -- {} {}'.format(symbol, execption))

    return None


def exec_schedule():
    today = datetime.datetime.today()
    return today.weekday() < 5 # 5:sat 6:sun


def save_chart(row, chart, savefig, days = 50):
    chart = chart.tail(days)

    adp = [
        mpf.make_addplot(chart['SUPERTl_10_1.0'], panel = 0, width = 1, color = 'green'),
        mpf.make_addplot(chart['SUPERTl_11_2.0'], panel = 0, width = 1, color = 'green'),
        mpf.make_addplot(chart['SUPERTl_12_3.0'], panel = 0, width = 1, color = 'green'),
        mpf.make_addplot(chart['SUPERTs_10_1.0'], panel = 0, width = 1, color = 'red'),
        mpf.make_addplot(chart['SUPERTs_11_2.0'], panel = 0, width = 1, color = 'red'),
        mpf.make_addplot(chart['SUPERTs_12_3.0'], panel = 0, width = 1, color = 'red'),
    ]

    mpf.plot(
        chart,
        title = '{} - {}'.format(row['symbol'], row['name']),
        addplot = adp,
        type = 'candle',
        style = 'yahoo',
        savefig = savefig
    )


def judge_stock(row):
    try:
        logger.info('{} - {}'.format(row['symbol'], row['name']))

        # 企業情報を取得
        info = call_with_retry(
            investpy.stocks.get_stock_information,
            stock = row['symbol'],
            country = 'japan'
        )

        # 監視対象のフィルタリング
        if info['Prev. Close'][0] > 7000 or info['Prev. Close'][0] < 500 or info['Volume'][0] < 70000:
            logger.debug(' -- {} ignore. price:{} volume:{}'.format(
                row['symbol'],
                info['Prev. Close'][0],
                info['Volume'][0]
            ))
            return

        # チャート情報取得
        chart = get_histrical_data(row['symbol'])
        if chart is None:
            return

        # EMA取得
        avg = call_with_retry(
            investpy.moving_averages,
            name = row['symbol'],
            country = 'japan',
            product_type='stock',
        )
        ema200 = avg.query('period == "200"')['ema_value'].values[0]

        # SuperTrend取得
        supertrend = ta.supertrend(
            high = chart['High'],
            low = chart['Low'],
            close = chart['Close'],
            length = 10,
            multiplier = 1.0
        )
        chart = pd.concat([chart, supertrend], axis = 1)
        supertrend = ta.supertrend(
            high = chart['High'],
            low = chart['Low'],
            close = chart['Close'],
            length = 11,
            multiplier = 2.0
        )
        chart = pd.concat([chart, supertrend], axis = 1)
        supertrend = ta.supertrend(
            high = chart['High'],
            low = chart['Low'],
            close = chart['Close'],
            length = 12,
            multiplier = 3.0
        )
        chart = pd.concat([chart, supertrend], axis = 1)

        # 売買判定
        buy  = (chart['SUPERTd_10_1.0'][-2] == -1 and chart['SUPERTd_10_1.0'][-1] == 1) and \
               (chart['SUPERTd_11_2.0'][-2] == -1 and chart['SUPERTd_11_2.0'][-1] == 1) and \
               (chart['SUPERTd_12_3.0'][-2] == -1 and chart['SUPERTd_12_3.0'][-1] == 1) and \
               (ema200 < chart['Close'][-1])
        sell = (chart['SUPERTd_10_1.0'][-2] == 1 and chart['SUPERTd_10_1.0'][-1] == -1) and \
               (chart['SUPERTd_11_2.0'][-2] == 1 and chart['SUPERTd_11_2.0'][-1] == -1) and \
               (chart['SUPERTd_12_3.0'][-2] == 1 and chart['SUPERTd_12_3.0'][-1] == -1) and \
               (ema200 > chart['Close'][-1])

        # 通知
        if sell or buy:
            logger.debug(' -- {} {}. price:{} volume:{}'.format(
                row['symbol'],
                'buy' if buy else 'sell',
                chart['Close'][-1],
                chart['Volume'][-1],
            ))

            # グラフ保存パス
            _, temp = tempfile.mkstemp(suffix='.png')

            # グラフ保存
            save_chart(row, chart, savefig = temp)

            # Lineへ通知
            message = ''
            message += '\nコード : {}'.format(row['symbol'])
            message += '\n銘柄名 : {}'.format(row['name'])
            message += '\n値幅 : {}'.format(info['Todays Range'][0])
            message += '\n決算予定日 : {}'.format(info['Next Earnings Date'][0])
            message += '\n始値 : {}'.format(chart['Open'].values[-1])
            message += '\n終値 : {}'.format(chart['Close'].values[-1])
            message += '\n高値 : {}'.format(chart['High'].values[-1])
            message += '\n安値 : {}'.format(chart['Low'].values[-1])
            message += '\n出来高 : {}'.format(chart['Volume'].values[-1])
            message += '\nEMA200 : {}'.format(ema200)
            message += '\nSuperTrend_10_1 : {}'.format(round(chart['SUPERT_10_1.0'][-1]))
            message += '\nSuperTrend_11_2 : {}'.format(round(chart['SUPERT_11_2.0'][-1]))
            message += '\nSuperTrend_12_3 : {}'.format(round(chart['SUPERT_12_3.0'][-1]))
            message += '\n判定 : {}'.format('買い' if buy else '売り')
            message += '\nhttps://m.finance.yahoo.co.jp/stock?code={}.T'.format(row['symbol'])

            with open(temp, 'rb') as f:
                line_notify(message, file = f)
            os.remove(temp)

        else:
            logger.debug(' -- {} not sell or buy. price:{} volume:{}'.format(
                row['symbol'],
                chart['Close'][-1],
                chart['Volume'][-1],
            ))
    except:
        etype, evalue, etraceback = sys.exc_info()
        execption = traceback.format_exception(etype, evalue, etraceback)
        logger.error(' -- {} {}'.format(row['symbol'], ''.join(execption)))

def line_notify(message, file = None):
    url = 'https://notify-api.line.me/api/notify'
    token = '0hjXEcp5X68Y3C3DSM4PTtZdx1rfBfJ2jnPGeil4H9a'
    headers = {'Authorization': 'Bearer ' + token}

    payload = {'message': message}
    files = {}

    if file is not None:
        files = {'imageFile': file}

    requests.post(url, headers = headers, params = payload, files = files)


def job():
    if exec_schedule():
        logger.info('start job')
        start_time = time.time()

        # 銘柄一覧を取得
        stocks = call_with_retry(
            investpy.stocks.get_stocks,
            country = 'japan'
        )
        logger.info('total stocks {}'.format(len(stocks.index)))

        if args.target != '':
            target = stocks.query('symbol == "{}"'.format(args.target))

            if len(target.index) > 0:
                judge_stock(target.to_dict(orient = 'records')[0])
            else:
                logger.warning('cannot found {}'.format(args.target))
        elif args.single:
            logger.info('single process')
            for s in stocks.to_dict(orient = 'records'):
                judge_stock(s)
        else:
            # 並列処理
            pool = Pool(4)
            pool.map(judge_stock, stocks.to_dict(orient = 'records'))

        elapsed_time = time.time() - start_time
        logger.info('end job. elapsed time {} sec'.format(elapsed_time))


if __name__=='__main__':

    # ログの設定
    setup_logger()
    logger.info('start script')

    if args.run:
        job()
    else:
        # スケジュールを設定
        schedule.every().day.at("00:00").do(job)

        # job実行ループ
        while True:
            schedule.run_pending()
            time.sleep(1)


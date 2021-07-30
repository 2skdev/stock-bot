#!/usr/bin/python3

# append path
import sys
site_package = '/home/tosaki/.local/lib/python3.7/site-packages'
if site_package not in sys.path:
    sys.path.append(site_package)

import os
import time
import logging
import requests
import datetime
import pandas as pd
import mplfinance as mpf
import tempfile
import schedule
import investpy
from concurrent import futures
from multiprocessing import Pool

# ログファイル
logger = logging.getLogger(__name__)

# tmpファイル
LOG_FILENAME = 'trace.log'
FIG_PATH = '{}.png'

# 通知銘柄リスト
notify_list = []

def setup_logger():
    path = os.path.dirname(os.path.abspath(__file__)) + '/' + LOG_FILENAME

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)s %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(filename = path)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger

def investpy_with_retry(func, *args, **kwargs):
    retry_max = 20

    for i in range(retry_max):
        try:
            return func(*args, **kwargs)
        except ConnectionError as e:
            if i == retry_max - 1:
                raise e
            else:
                logger.info(' -- {} retry {}/10'.format(func.__name__, i + 1))
                time.sleep(1)

def exec_schedule():
    today = datetime.datetime.today()
    return today.weekday() < 5 # 5:sat 6:sun

def calc_rsi(data):
    diff = data.diff()[1:]

    up = diff.mask(diff < 0, 0)
    down = diff.mask(diff > 0, 0).abs()

    up_sma_9 = up.rolling(window = 9).mean()
    down_sma_9 = down.rolling(window = 9).mean()

    up_sma_14 = up.rolling(window = 14).mean()
    down_sma_14 = down.rolling(window = 14).mean()

    return pd.DataFrame({
        'RSI_9': up_sma_9 / (up_sma_9 + down_sma_9) * 100,
        'RSI_14': up_sma_14 / (up_sma_14 + down_sma_14) * 100,
    })

def calc_macd(data, short_term = 6, long_term = 19, signal_term = 9):
    ema_short = data.ewm(span = short_term).mean()
    ema_long = data.ewm(span = long_term).mean()

    macd = ema_short - ema_long
    signal = macd.ewm(span = signal_term).mean()

    return pd.DataFrame({
        'MACD': macd,
        'Signal': signal,
    })

def judge_sellbuy(chart, pbr):
    diff_macd    = chart['MACD'] > chart['Signal']              # SignalよりMACDが上
    cross_macd   = diff_macd ^ diff_macd.shift()                # SignalとMACDが交差
    golden_cross = (cross_macd == True) & (diff_macd == True)   # SignalとMACDが交差してMACDが上
    dead_cross   = (cross_macd == True) & (diff_macd == False)  # SignalとMACDが交差してMACDが下

    sell = (dead_cross.values[-1]) and \
           (True not in golden_cross.tail(9).values) and \
           (chart['MACD'].values[-1] > 0) and \
           (chart['RSI_9'].tail(9).max() > 75) and \
           (chart['RSI_14'].values[-1] > chart['RSI_9'].values[-1]) and \
           (chart['RSI_9'].values[-1] > 50) and \
           (pbr > 1.5)

    buy  = (golden_cross.values[-1]) and \
           (True not in dead_cross.tail(9).values) and \
           (chart['MACD'].values[-1] < 0) and \
           (chart['RSI_9'].tail(9).min() < 25) and \
           (chart['RSI_14'].values[-1] < chart['RSI_9'].values[-1]) and \
           (chart['RSI_9'].values[-1] < 50) and \
           (pbr < 5)

    return sell, buy

def save_chart(code, name, chart, sell, buy, days = 50):
    apd = [
        mpf.make_addplot(chart['MACD'],   panel = 'lower', color = 'r', linestyle = 'dashed', width = 1, ylabel = 'MACD'),
        mpf.make_addplot(chart['Signal'], panel = 'lower', color = 'g', linestyle = 'dashed', width = 1, ylabel = 'MACD'),
    ]

    fig, axes = mpf.plot(
        chart,
        addplot = apd,
        volume = True,
        type = 'candle',
        style = 'yahoo',
        mav = (5, 25, 75),
        figratio = (10, 5),
        returnfig = True
    )

    axes[0].set_title('{} - {}'.format(code, name))
    axes[0].set_ylim(chart.tail(days)['Low'].min(), chart.tail(days)['High'].max())
    axes[1].set_ylim(chart.tail(days)['Low'].min(), chart.tail(days)['High'].max())
    for ax in axes:
        ax.set_xlim(len(chart.index) - days, len(chart.index) - 1)

    fig.savefig(FIG_PATH.format(code))

def judge_stock(row):
    logger.info('{} - {}'.format(row['symbol'], row['name']))

    # 企業情報を取得
    info = investpy_with_retry(
        investpy.stocks.get_stock_information,
        stock = row['symbol'],
        country = 'japan'
    )

    # 監視対象フィルタリング
    if info['Prev. Close'][0] > 7000 or info['Prev. Close'][0] < 700 or info['Volume'][0] < 10000:
        logger.info(' -- ignore. price:{} volume:{}'.format(info['Prev. Close'][0], info['Volume'][0]))
        return

    # チャート情報を取得
    try:
        # 期間
        end   = datetime.date.today()
        start = datetime.date.today() - datetime.timedelta(days = 200)
        end   = datetime.datetime.strftime(end,   '%d/%m/%Y')
        start = datetime.datetime.strftime(start, '%d/%m/%Y')

        chart = investpy_with_retry(
            investpy.stocks.get_stock_historical_data,
            stock = row['symbol'],
            country = 'japan',
            from_date = start,
            to_date = end,
        )
    except IndexError as e:
        logger.warning(e)
        return
    except RuntimeError as e:
        logger.warning(e)
        return
    except ConnectionError as e:
        logger.warning(e)
        return

    # オシレーター情報を計算
    chart = pd.concat([chart, calc_rsi(chart['Close']), calc_macd(chart['Close'])], axis = 1)

    # 決算情報を取得
    financial = investpy_with_retry(
        investpy.stocks.get_stock_financial_summary,
        stock = row['symbol'],
        country = 'japan',
        summary_type = 'balance_sheet',
        period = 'quarterly'
    )

    # PER, PBRを計算
    pbr = info['Prev. Close'][0] / (financial['Total Equity'][0] * 1000000 / info['Shares Outstanding'][0])

    # 売買判定
    sell, buy = judge_sellbuy(chart, pbr)

    # 戻り値
    result = {
        'Code': row['symbol'],
        'Name': row['name'],
        'Range': info['Todays Range'][0],
        'Earnings': info['Next Earnings Date'][0],
        'PBR': round(pbr, 2),
        'Open': chart['Open'].values[-1],
        'Close': chart['Close'].values[-1],
        'High': chart['High'].values[-1],
        'Low': chart['Low'].values[-1],
        'Volume': chart['Volume'].values[-1],
        'RSI_9': round(chart['RSI_9'].values[-1], 2),
        'RSI_14': round(chart['RSI_14'].values[-1], 2),
        'MACD': round(chart['MACD'].values[-1], 2),
        'Signal': round(chart['Signal'].values[-1], 2),
        'Sell': sell,
        'Buy': buy,
    }

    if sell or buy:
        # チャート画像を生成
        save_chart(row['symbol'], row['name'], chart, sell, buy)

        for k, v in result.items():
            logger.info(' -- {}: {}'.format(k, v))

    return result

def line_notify(message, file = None):
    url = 'https://notify-api.line.me/api/notify'
    token = '0hjXEcp5X68Y3C3DSM4PTtZdx1rfBfJ2jnPGeil4H9a'
    headers = {'Authorization': 'Bearer ' + token}

    payload = {'message': message}

    if file is not None:
        files = {'imageFile': file}
    else:
        files = {}

    requests.post(url, headers = headers, params = payload, files = files)

def search_stock_job():
    logger.info('start search_stock_job')

    if exec_schedule():
        # 通知銘柄リストをクリア
        notify_list.clear()

        # 東証上場銘柄のExcelを取得する
        stocks = investpy_with_retry(
            investpy.stocks.get_stocks,
            country = 'japan'
        )
        logger.info('total stocks {}'.format(len(stocks.index)))

        # 並列処理
        pool = Pool(4)
        result = pool.map(judge_stock, stocks.to_dict(orient='records'))

        # 結果を取得
        for r in result:
            if r is not None:
                # 売り or 買い判定のみ通知
                if r['Sell'] or r['Buy']:
                    notify_list.append(r)

def notify_result_job():
    logger.info('start notify_result_job')

    if exec_schedule():
        for stock in notify_list:
            message = ''
            message += '\nコード : {}'.format(stock['Code'])
            message += '\n銘柄名 : {}'.format(stock['Name'])
            message += '\n値幅 : {}'.format(stock['Range'])
            message += '\n決算予定日 : {}'.format(stock['Earnings'])
            message += '\nPBR : {}'.format(stock['PBR'])
            message += '\n始値 : {}'.format(stock['Open'])
            message += '\n終値 : {}'.format(stock['Close'])
            message += '\n高値 : {}'.format(stock['High'])
            message += '\n安値 : {}'.format(stock['Low'])
            message += '\n出来高 : {}'.format(stock['Volume'])
            message += '\nRSI短期 : {}'.format(stock['RSI_9'])
            message += '\nRSI長期 : {}'.format(stock['RSI_14'])
            message += '\nMACD : {}'.format(stock['MACD'])
            message += '\nシグナル : {}'.format(stock['Signal'])
            message += '\n売り判定 : {}'.format(stock['Sell'])
            message += '\n買い判定 : {}'.format(stock['Buy'])
            message += '\nhttps://m.finance.yahoo.co.jp/stock?code={}.T'.format(stock['Code'])

            line_notify(message, file = open(FIG_PATH.format(stock['Code']), 'rb'))

if __name__ == '__main__':
    logger.info('start script')

    # ログの設定
    setup_logger()

    with tempfile.TemporaryDirectory() as temp:
        # tmpファイルの生成
        FIG_PATH = temp + '/' + FIG_PATH
        logger.info(FIG_PATH)

        # スケジュールを設定
        schedule.every().day.at("00:00").do(search_stock_job)
        schedule.every().day.at("06:00").do(notify_result_job)

        # # job実行ループ
        while True:
            schedule.run_pending()
            time.sleep(1)

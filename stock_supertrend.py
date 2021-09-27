
import logging

import time
import datetime

import pandas as pd
import pandas_ta as ta
import investpy

import mplfinance as mpf

# ロギング
logger = logging.getLogger(__name__)


def setup_logger():
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(process)d %(levelname)s %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

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
                logger.info(' -- {} retry {}/10'.format(func.__name__, i + 1))
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
    except IndexError as e:
        logger.warning(e)
        return None
    except RuntimeError as e:
        logger.warning(e)
        return None
    except ConnectionError as e:
        logger.warning(e)
        return None


def save_chart(row, chart, days = 50):
    chart = chart.tail(days)

    adp = [
        mpf.make_addplot(chart['SUPERTl_10_3.0'], panel = 0, width = 1, color = 'green'),
        mpf.make_addplot(chart['SUPERTs_10_3.0'], panel = 0, width = 1, color = 'red'),
    ]

    mpf.plot(
        chart,
        title = '{} - {}'.format(row['symbol'], row['name']),
        addplot = adp,
        type = 'candle',
        style = 'yahoo',
        savefig = './hoge.png'
    )


def judge_stock(row):
    logger.info('{} - {}'.format(row['symbol'], row['name']))

    # 企業情報を取得
    info = call_with_retry(
        investpy.stocks.get_stock_information,
        stock = row['symbol'],
        country = 'japan'
    )

    # 決算情報を取得
    financial = call_with_retry(
        investpy.stocks.get_stock_financial_summary,
        stock = row['symbol'],
        country = 'japan',
        summary_type = 'balance_sheet',
        period = 'quarterly'
    )

    # 監視対象のフィルタリング
    if info['Prev. Close'][0] > 7000 or info['Prev. Close'][0] < 1000 or info['Volume'][0] < 100000:
        logger.info(' -- ignore. price:{} volume:{}'.format(info['Prev. Close'][0], info['Volume'][0]))
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
    ema200 = avg.query('period == "200"')['ema_value']

    # SuperTrend取得
    supertrend = ta.supertrend(
        high = chart['High'],
        low = chart['Low'],
        close = chart['Close'],
        length = 10,
        multiplier = 3.0
    )
    chart = pd.concat([chart, supertrend], axis = 1)

    # 売買判定
    buy  = (chart['SUPERTd_10_3.0'][-2] == -1 and chart['SUPERTd_10_3.0'][-1] ==  1) and (ema200 < chart['Close'][-1])
    sell = (chart['SUPERTd_10_3.0'][-2] ==  1 and chart['SUPERTd_10_3.0'][-1] == -1) and (ema200 > chart['Close'][-1])
    if sell or buy:
        logger.info('\nbuy:{}\nsell:{}\nopen:{}\nclose:{}\nhigh:{}\nlow:{}\n'.format(
            buy, sell,
            chart['Open'][-1],
            chart['Close'][-1],
            chart['High'][-1],
            chart['Low'][-1],
            ema200,
            chart['SUPERT_10_3.0'][-1],
        ))

    # グラフ保存
    save_chart(row, chart)


if __name__=='__main__':
    setup_logger()

    # 銘柄一覧を取得
    stocks = call_with_retry(
        investpy.stocks.get_stocks,
        country = 'japan'
    )

    stocks = stocks.to_dict(orient='records')
    judge_stock(stocks[0])

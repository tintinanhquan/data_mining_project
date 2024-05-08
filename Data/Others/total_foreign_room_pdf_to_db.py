import io
import re
import json
import logging
import sys
import time

import requests
from datetime import datetime

from bs4 import BeautifulSoup
import PyPDF2

from database.entity.trading import Volume
from database.manager.quote_manager import QuoteManager

logging.basicConfig(
    format='%(asctime)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

VSD_NEWS_URL = 'https://vsd.vn/vi/ad/'


def get_total_foreign_pdf_file():
    with open(NEWS_DATA_FILEPATH, 'r') as data_file:
        news_id = json.load(data_file)['previous_news_id']

    previous_news_id = news_id
    while True:
        news_id += 1
        news_url = VSD_NEWS_URL + str(news_id)
        news_page = BeautifulSoup(requests.get(news_url).text, 'html.parser')

        news_title = news_page.select(".title-category")[0].get_text()
        logging.info(f'{news_id}: {news_title}')

        if 'Công bố thông tin về tỷ lệ sở hữu nước ngoài ngày' in news_title:
            break

        if news_id - previous_news_id > 100:
            return None, None

        time.sleep(1)

    the_date = datetime.strptime(
        re.search('../../....', news_title)[0],
        '%d/%m/%Y'
    ).date()

    with open(NEWS_DATA_FILEPATH, 'w') as data_file:
        json.dump({
            'previous_news_id': news_id
        }, data_file)

    total_foreign_room_pdf_url = [
        link['href'] for link in news_page.select('a')
        if link.has_attr('href') and '.pdf' in link['href']
    ][0]
    return the_date, io.BytesIO(requests.get(total_foreign_room_pdf_url).content)


if __name__ == "__main__" and len(sys.argv) == 2:
    NEWS_DATA_FILEPATH = sys.argv[1]

    tickers = QuoteManager().all_tickers()
    ticker_symbols = [ticker.ticker_symbol for ticker in tickers]

    affected_date, pdfIO = get_total_foreign_pdf_file()

    if pdfIO is None:
        sys.exit()

    pdfReader = PyPDF2.PdfReader(pdfIO)
    cells = []
    for page in pdfReader.pages:
        cells += re.split('\n| ', page.extract_text())

    list_entity = []
    list_added_ticker_symbols = []
    for i, cell_text in enumerate(cells):
        if len(cell_text) == 0 or cell_text[-1] != '%':
            continue

        ticker_symbol = cells[i - 1].replace(' ', '').replace('.', '')
        if ticker_symbol.isdigit():
            continue

        total_foreign_room = cells[i + 1].replace('.', '')
        if not total_foreign_room.isdigit():
            continue

        total_foreign_room = int(total_foreign_room)
        if ticker_symbol in ticker_symbols and ticker_symbol not in list_added_ticker_symbols:
            list_added_ticker_symbols.append(ticker_symbol)
            list_entity.append(Volume(
                ticker=ticker_symbol,
                datetime=affected_date,
                quantity=total_foreign_room,
                data_item='totalforeignroom'
            ))
            # logging.info(f'{ticker_symbol}, {total_foreign_room}')

    QuoteManager().add_list_entity(list_entity=list_entity, overwrite=True)
    logging.info(f'Added total foreign room of date {affected_date} to database.')
    logging.info('Finished.')

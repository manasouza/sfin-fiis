import os
import sys
import logging
import yaml
import re
import time

import scrapy
from scrapy.crawler import CrawlerProcess
from google.cloud import storage
from gspreadsheet import SpreadsheetIntegration
from datetime import datetime


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

storage_client = storage.Client()

spreadsheet = None
fiis = {}

with open("config.yaml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
SPREADSHEET_TICKETS_TAB = config["spreadsheet"]["tickers_tab"]
CRAWLER_SITE_COMPONENT = config["crawler"]["site_component"]
SPREADSHEET_DY_TAB = config["spreadsheet"]["dy_tab"]["name"]
HEADER_ROW = config["spreadsheet"]["dy_tab"]["header_row"]
TICKERS_COLUMN_INDEX = config["spreadsheet"]["dy_tab"]["tickers_column"]["index"]
DATE_COLUMN_INDEX = config["spreadsheet"]["dy_tab"]["date_column"]["index"]
VALUE_COLUMN_INDEX = config["spreadsheet"]["dy_tab"]["dy_value_column"]["index"]
DATE_COLUMN_NAME = config["spreadsheet"]["dy_tab"]["date_column"]["name"]
TICKERS_COLUMN_NAME = config["spreadsheet"]["dy_tab"]["tickers_column"]["name"]
VALUE_COLUMN_NAME = config["spreadsheet"]["dy_tab"]["dy_value_column"]["name"]
DY_HEADER_ROW = config["spreadsheet"]["dy_tab"]["header_row"]
DY_LAST_COLUMN = config["spreadsheet"]["dy_tab"]["last_column"]
DY_DATE_FORMAT = config["spreadsheet"]["dy_tab"]["date_column"]["format"]
TOTAL_COLUMN = config["spreadsheet"]["dy_tab"]["total_column"]["index"]
DY_REF_COLUMN = config["spreadsheet"]["dy_tab"]["dy_avg_column"]["index"]
DAYS_LIMIT = config["search"]["before_days_limit"]


class FiisComBrSpider(scrapy.Spider):

    name = 'fiis'
    allowed_domains = ['fiis.com.br']

    def __init__(self, fii='fii_code'):
        logger = logging.getLogger('scrapy.statscollectors')
        logger.setLevel(logging.ERROR)
        logger = logging.getLogger('scrapy.core.engine')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.middleware')
        logger.setLevel(logging.WARNING)
        logger = logging.getLogger('scrapy.extensions.telnet')
        logger.setLevel(logging.WARNING)
        logger.info('###############################################################')
        logger.info('starting crawler processing')
        logger.info('###############################################################')
        super(FiisComBrSpider, self).__init__()
        self.url = 'https://fiis.com.br/%s' % fii

    def start_requests(self):
        url=self.url
        yield scrapy.Request(url, errback=self.errback_httpbin)

    def errback_httpbin(self, failure):
        """ Ref: https://docs.scrapy.org/en/latest/topics/request-response.html
        Args:
            failure (_type_): _description_
        """
        # TODO: exponential backoff to try to request again those failed requests
        print('###############################################################')
        logging.error(failure.value.response)
        print('###############################################################')


    def parse(self, response):
        logging.info("procesing: "+response.url)
        fii_code = response.url.split('/')[-2]
        fii_updates = response.xpath(CRAWLER_SITE_COMPONENT).extract()
        extracted_value, extracted_ref_date = self._extract_dyvalue_and_date_v2(fii_updates)
        add_fii_dy_data(fii_code, extracted_value, extracted_ref_date)

    def _extract_dyvalue_and_date_v2(self, fii_updates, extracted_value='', extracted_ref_date=''):
        """Extract DY values from website "v2"

           fii_updates represents the content of the extracted website piece. It's a table with history of DY revenues
            * It's sliced from index 2 to eliminate the part of the title (i.e. ÚLTIMOS DIVIDENDOS DO VINO11)
            * There are five columns: Data Base, Data Pagamento, Cotação Base, Dividend Yield, Rendimento
            ** First registry represents most recent values
            ** Since the table values are into a list, 5 and 9 are the index of first occurence of date and DY value

        Returns:
            Tuple: DY value, Date reference value
        """
        revenue_elements = [f.replace('\n', '').rstrip() for f in fii_updates if f != '\n' and f != '\n '][2:]
        for index,element in enumerate(revenue_elements):
            value_found = re.search('R\$\s(\d?,?\d+|)', element)
            if value_found and index==9:
                extracted_value = value_found.group(1)
            date_found = re.search('(\d{2}\.\d{2}\.\d{4})', element)
            if date_found and index==5:
                extracted_ref_date = date_found.group(1)
            if extracted_ref_date and extracted_value:
                return extracted_value, extracted_ref_date.replace('.', '/')
        return '',''

def setup_spreadsheet(spreadsheet_id, credentials_path):
    global spreadsheet
    spreadsheet = SpreadsheetIntegration(spreadsheet_id, cred_file_path=credentials_path)

def add_fii_dy_data(fii_code: str, dy_value: str, dy_base_date: str):
    fiis[fii_code] = {
        'value': dy_value,
        'date': dy_base_date
    }

def any_fii_extracted(fiis: dict):
    return True if [f for f in fiis.keys() if fiis[f]['value'] != ''] else False

def worksheet_state(original_fiis_length):
    """
        Args:
            worksheet (_type_): _description_
            original_fiis_length (_type_): _description_
        Returns:
            Tuple: starting_point, starting_point, dy_value_cells, fiis_not_registered
    """
    dy_value_cell_header = spreadsheet.find(VALUE_COLUMN_NAME, from_row=DY_HEADER_ROW)

    import ipdb; ipdb.set_trace()

    starting_point = dy_value_cell_header.row + 1
    dy_ticker_cell_header = spreadsheet.find(TICKERS_COLUMN_NAME, from_row=DY_HEADER_ROW)
    next_row_to_be_filled = (dy_value_cell_header.row + original_fiis_length)
    dy_value_cells = spreadsheet.get_cells_in_the_range(starting_point, dy_value_cell_header.col, starting_point, dy_value_cell_header.col)
    dy_ticker_cells = spreadsheet.get_cells_in_the_range(starting_point, dy_ticker_cell_header.col, starting_point, dy_ticker_cell_header.col)
    dy_ticker_cell_values = [d.value.upper() for d in dy_ticker_cells]
    return starting_point, next_row_to_be_filled, dy_value_cells, dy_ticker_cell_values

def check_registration_state(registered_tickers, fiis_list, dy_value_filled_cells):
    """_summary_

    Args:
        registered_tickers (_type_): _description_
        fiis_list (_type_): _description_
        dy_value_filled_cells (_type_): _description_

    Returns:
        _type_: _description_
    """
    fiis_not_registered = [f for f in fiis_list if f not in registered_tickers]
    logging.info(f'FIIs not registered yet: {fiis_not_registered}')
    total_expected_fiis = len(fiis_list)
    total_found_fiis = len(fiis.keys())
    if total_expected_fiis != total_found_fiis:
        logging.info(f'Expected FII list length ({total_expected_fiis}) does not match FIIs found ({total_found_fiis})\n')
        fiis_expected_but_not_found = [fii for fii in fiis_list if fii.lower() not in fiis.keys()]
        logging.info(f'Not found during discovery: {fiis_expected_but_not_found}')
    # TODO: get current month and compare with month from recent FIIs
    filled_cells_count = len([True for c in dy_value_filled_cells if c.value != ''])
    logging.info(f'Already filled cells count: {filled_cells_count}')
    logging.info(f'FIIs found count: {total_found_fiis}')
    logging.info(f'FIIs expected count: {total_expected_fiis}')
    return filled_cells_count

def insert_blank_row_set(starting_point, next_row_to_be_filled, original_fiis_length, filled_cells_count, dy_value_filled_cells):
    """
        Create new set of rows only if all rows in the FII range are filled, based in the original total FIIs length
        Args:
            None
        Returns:
            bool: True if blank row should be inserted, False otherwise
    """
    if original_fiis_length is not None and filled_cells_count >= original_fiis_length:
        spreadsheet.insert_rows(dy_value_filled_cells, next_row_to_be_filled, value_input_option='RAW')
        # create total sum cell
        spreadsheet.update_cell(starting_point, TOTAL_COLUMN, ''.join(['=SUM(C',str(next_row_to_be_filled),':C',str(starting_point),')']))
        # most_recent_date = max((date_values), key=lambda x: datetime.strptime(x, "%d/%m/%Y"))
        spreadsheet.update_cell(starting_point, DY_REF_COLUMN, ''.join(['=AVERAGE(G',str(next_row_to_be_filled),':G',str(starting_point),')']))
        # set bold
        spreadsheet.format(''.join(['H',str(starting_point),':I',str(starting_point)]), {'textFormat': {'bold': True}})
        # TODO: copy/paste for formula cells on past month

def check_dividend_yield(fiis_list=[], limited=True):
    """
        Args:
            fiis_list (list, optional): list of FII code to process, if informed. Defaults to [].
            limited (bool, optional): Delimit days count from today and if true, it will confront with configured limit days to process FIIs data. Defaults to True.
    """
    original_fiis_length = None
    if not fiis:
        spreadsheet.set_worksheet(SPREADSHEET_TICKETS_TAB)
        original_fiis_list = [ticker for ticker in spreadsheet.get_column_values(TICKERS_COLUMN_INDEX-1) if re.search('\w+11', ticker)]
        original_fiis_length = len(original_fiis_list)
        if not fiis_list:
            fiis_list = original_fiis_list
            logging.info('FIIs to be processed %s' % fiis_list)

        process = CrawlerProcess({
                # 'DOWNLOAD_DELAY': 10,
                'CONCURRENT_REQUESTS' : 1,
                'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
                })
        for fii in fiis_list:
            process.crawl(FiisComBrSpider, fii=fii)
        process.start()
    logging.info(f'\nProcessing {len(fiis)} FIIs')
    if not any_fii_extracted(fiis):
        logging.warning('could not extract fiis from source. Check for source website updates.')
        sys.exit()

    for k,v in fiis.items():
        logging.info(f'FII: {k} => R$ {v["value"]} em {v["date"]}')

    spreadsheet.set_worksheet(SPREADSHEET_DY_TAB)
    starting_point, next_row_to_be_filled, dy_value_filled_cells, registered_tickers = worksheet_state(original_fiis_length)

    filled_cells_count = check_registration_state(registered_tickers, fiis_list, dy_value_filled_cells)
    insert_blank_row_set(starting_point, next_row_to_be_filled, original_fiis_length, filled_cells_count, dy_value_filled_cells)

    # update DY values
    update_index = starting_point
    for k,v in fiis.items():
        if next_row_to_be_filled < update_index:
            logging.info('limit reached, but there still FIIs to process')
            break
        if spreadsheet.fii_processed(k, starting_point, next_row_to_be_filled, TICKERS_COLUMN_INDEX):
            continue
        update_index = spreadsheet.next_empty_row(update_index, next_row_to_be_filled, VALUE_COLUMN_INDEX)
        if update_index == None:
            raise ValueError('Fail to update row based in ' + str(next_row_to_be_filled) + ' for ' + k)
        if v['date'] == '' and v['value'] == '':
            logging.info(f'no DY info for FII: {k}')
        today = datetime.today()
        fii_close_date = v['date'] if v['date'] != '' else today.strftime(DY_DATE_FORMAT)
        ffii_close_date = datetime.strptime(fii_close_date, DY_DATE_FORMAT)
        if limited:
            ftoday = datetime.strptime(today.strftime(DY_DATE_FORMAT), DY_DATE_FORMAT)
            numberofdays = (ftoday - ffii_close_date).days
        else:
            numberofdays = 0
        if numberofdays <= DAYS_LIMIT:
            spreadsheet.update_cell(update_index, TICKERS_COLUMN_INDEX, k)
            spreadsheet.update_cell(update_index, VALUE_COLUMN_INDEX, v['value'] if v['value'] != '' else 0)
            spreadsheet.update_cell(update_index, DATE_COLUMN_INDEX, fii_close_date)
        else:
            logging.info(f'waiting next iterarion for {k}')
        # delay to avoid HTTP 429
        time.sleep(5)
import scrapy
import logging
import re
import yaml
from scrapy.crawler import CrawlerProcess

with open("config.yaml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
CRAWLER_CONFIG = config["crawler"]
CRAWLER_SITE_COMPONENT = CRAWLER_CONFIG["site_component"]
DOWNLOAD_DELAY = CRAWLER_CONFIG.get("download_delay", 3)
RANDOMIZE_DOWNLOAD_DELAY = CRAWLER_CONFIG.get("randomize_download_delay", True)
RETRY_TIMES = CRAWLER_CONFIG.get("retry_times", 3)

fiis = {}

def add_fii_dy_data(fii_code: str, dy_value: str, dy_base_date: str):
    fiis[fii_code.upper()] = {
        'value': dy_value,
        'date': dy_base_date
    }

def search_fii_dividends(fiis_list: list) -> dict:
    """
    Run FiisComBrSpider for each requested FII and return the extracted DY data.
    """
    fiis.clear()
    if not fiis_list:
        logging.warning('No FIIs provided for webscraping.')
        return {}

    crawler_settings = {
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_DELAY': DOWNLOAD_DELAY,
        'RANDOMIZE_DOWNLOAD_DELAY': RANDOMIZE_DOWNLOAD_DELAY,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': RETRY_TIMES,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504, 522, 524, 408],
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
    }
    process = CrawlerProcess(crawler_settings)
    process.crawl(FiisComBrSpider, fiis=fiis_list)
    process.start()

    return dict(fiis)

class FiisComBrSpider(scrapy.Spider):

    name = 'fiis'
    allowed_domains = ['fiis.com.br']

    def __init__(self, fii='fii_code', fiis=None):
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
        self.fiis = [ticker.upper() for ticker in (fiis or [fii])]

    def start_requests(self):
        for fii in self.fiis:
            url = 'https://fiis.com.br/%s' % fii
            yield scrapy.Request(url, errback=self.errback_httpbin, cb_kwargs={'fii': fii})

    def errback_httpbin(self, failure):
        """ Ref: https://docs.scrapy.org/en/latest/topics/request-response.html
        Args:
            failure (_type_): _description_
        """
        # TODO: exponential backoff to try to request again those failed requests
        print('###############################################################')
        logging.error(failure.value.response)
        print('###############################################################')


    def parse(self, response, fii):
        logging.info("procesing: "+response.url)
        fii_updates = response.xpath(CRAWLER_SITE_COMPONENT).extract()
        extracted_value, extracted_ref_date = self._extract_dyvalue_and_date_v2(fii_updates)
        add_fii_dy_data(fii, extracted_value, extracted_ref_date)

    def _extract_dyvalue_and_date_v2(self, fii_updates, extracted_value='', extracted_ref_date=''):
        """Extract DY values from website "v2"

           fii_updates represents the content of the extracted website piece. It's a table with history of DY revenues
            * There are six columns: Tipo, Data Base, Data Pagamento, Cotação Base, Dividend Yield, Rendimento
            ** First registry represents most recent values

        Returns:
            Tuple: DY value, Date reference value
        """
        revenue_elements = [element.strip() for element in fii_updates if element.strip()]
        columns = ['Tipo', 'Data Base', 'Data Pagamento', 'Cotação Base', 'Dividend Yield', 'Rendimento']
        column_count = len(columns)

        header_start = next(
            (
                index for index in range(len(revenue_elements) - column_count + 1)
                if revenue_elements[index:index + column_count] == columns
            ),
            None
        )
        if header_start is None:
            logging.warning('Could not find dividends table header in scraped content.')
            return '', ''

        rows = revenue_elements[header_start + column_count:]
        for index in range(0, len(rows), column_count):
            row = rows[index:index + column_count]
            if len(row) < column_count:
                continue

            date_found = re.search(r'\d{2}\.\d{2}\.\d{4}', row[1])
            value_found = re.search(r'R\$\s*(\d+(?:,\d+)?)', row[5])
            if date_found and value_found:
                extracted_ref_date = date_found.group(0).replace('.', '/')
                extracted_value = value_found.group(1)
                return extracted_value, extracted_ref_date

        return '', ''

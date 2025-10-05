import scrapy
import logging
import re
import yaml

with open("config.yaml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
CRAWLER_SITE_COMPONENT = config["crawler"]["site_component"]

fiis = {}

def add_fii_dy_data(fii_code: str, dy_value: str, dy_base_date: str):
    fiis[fii_code] = {
        'value': dy_value,
        'date': dy_base_date
    }

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

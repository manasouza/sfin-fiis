import logging
import re
import yaml
import json
from datetime import datetime, timedelta

from tools.gspreadsheet import SpreadsheetIntegration

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

with open("config.yaml") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
SPREADSHEET_TICKETS_TAB = config["spreadsheet"]["tickers_tab"]
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
CREWAI_CONFIG = config.get("crewai", {})

class Workflow:
    def __init__(self, mode: str, spreadsheet: SpreadsheetIntegration):
      self.mode = mode
      self.spreadsheet = spreadsheet

    def validate_input(self, fiis_data: dict):
      self.spreadsheet.set_worksheet(SPREADSHEET_TICKETS_TAB)
      self.original_fiis_list = [ticker for ticker in self.spreadsheet.get_column_values(TICKERS_COLUMN_INDEX-1) if re.search('\w+11', ticker)]
      logging.info(f'Total registered FIIs: {len(self.original_fiis_list)}')

    def check_spreadsheet_state(self):
      """
      Checks the current state of the spreadsheet in terms of FIIs registered to determine where to insert new data.
      """
        # most_recent_date = max((date_values), key=lambda x: datetime.strptime(x, "%d/%m/%Y"))
      self.spreadsheet.set_worksheet(SPREADSHEET_DY_TAB)
      original_fiis_length = len(self.original_fiis_list)
      dy_ticker_cell_column = self.spreadsheet.find(TICKERS_COLUMN_NAME, from_row=DY_HEADER_ROW).col
      dy_value_cell_header = self.spreadsheet.find(VALUE_COLUMN_NAME, from_row=DY_HEADER_ROW)
      starting_point = dy_value_cell_header.row + 1
      logging.info(f'Starting point to fill DY values: {starting_point}')
      # next_row_to_be_filled = (dy_value_cell_header.row + original_fiis_length)
      last_row = (dy_value_cell_header.row + original_fiis_length)
      dy_value_cells = self.spreadsheet.get_cells_in_the_range(starting_point, dy_value_cell_header.col, last_row, dy_value_cell_header.col)
      dy_ticker_cells = self.spreadsheet.get_cells_in_the_range(starting_point, dy_ticker_cell_column, last_row, dy_ticker_cell_column)
      fiis_valid = [d.value.upper() for index,d in enumerate(dy_ticker_cells) if d.value != '' and dy_value_cells[index].value != 'R$ 0,00']
      fiis_registered = [(index, d.value.upper()) for index, d in enumerate(dy_ticker_cells) if d.value != '']
      logging.info(f'FIIs already registered in DY worksheet: {fiis_registered}')
      next_row_to_be_filled = len(fiis_registered) + starting_point
      if next_row_to_be_filled >= (original_fiis_length + starting_point):
        logging.info('FIIs limit reached, creating new rows for next round')
        # TODO: copy/paste for formula cells on past month
        dy_qty_cell_range = ''.join(['C',str(starting_point),':D',str(last_row)])
        dy_qty_values = self.spreadsheet.copy_cells(dy_qty_cell_range)
        dy_stats_cell_range = ''.join(['F',str(starting_point),':L',str(last_row)])
        dy_stats_values = self.spreadsheet.copy_cells(dy_stats_cell_range)

        fiis_registered.append('Total')
        self.spreadsheet.insert_rows(fiis_registered, from_row=starting_point)
        # create total sum cell
        last_row_with_total = last_row + 1

        self.spreadsheet.paste_cells(dy_qty_cell_range, dy_qty_values)
        self.spreadsheet.paste_cells(dy_stats_cell_range, dy_stats_values)
        self.spreadsheet.update_cell(last_row_with_total, TOTAL_COLUMN, '=SUM({}:{})', from_range_row=starting_point, to_range_row=last_row)
        self.spreadsheet.update_cell(last_row_with_total, DY_REF_COLUMN, '=AVERAGE({}:{})', from_range_row=starting_point, to_range_row=last_row)
        # set bold and horizontal line for total row
        self.spreadsheet.format(''.join(['A',str(last_row_with_total),':A',str(last_row_with_total)]),
                                {
                                  'textFormat': {'bold': True},'borders': {
                                  'bottom': {
                                      'style': 'SOLID',
                                      'width': 1,
                                      'color': {'red': 0, 'green': 0, 'blue': 0}
                                  }
                              }})
      else:
        logging.info(f'Next row to be filled: {next_row_to_be_filled}')
      return starting_point, next_row_to_be_filled, fiis_valid, fiis_registered

    def register_fiis(self, fiis_data: dict, fiis_registered: dict, next_row_to_be_filled=None):
      """
      fiis_data: records to be saved
      registered_fiis: records already saved to compare whether to save or not
      """
      # TODO: validate if that fits for the generic class model (copied from CollectedDataWorkflow)
      # logging.info(f'\nProcessing {len(fiis)} FIIs')
      if isinstance(fiis_data, dict):
        fiis = fiis_data
      else:
        fiis = json.loads(fiis_data)
      logging.info(f'\nProcessing {len(fiis)} FIIs')
      for ticker, fii_data in fiis.items():
        logging.info(f'FII: {ticker} => R$ {fii_data["value"]} em {fii_data["date"]}')
        # check if FII is already registered but has zero value filled
        if ticker in [fii for index,fii in fiis_registered]:
          # fii_index = [index for index,fii in fiis_registered].index(ticker)
          fii_index = next((index for index,c in fiis_registered if c == ticker), None)
          fii_row_in_spreadsheet = fii_index + (HEADER_ROW + 1)
          existing_value = self.spreadsheet.get_cell_value(fii_row_in_spreadsheet, VALUE_COLUMN_INDEX)
          if existing_value == '' or existing_value == 'R$ 0,00':
            logging.info(f'FII {ticker} already registered with value {existing_value}: needs to be overwritten')
            next_row_to_be_filled = fii_row_in_spreadsheet
        self.spreadsheet.update_cell(next_row_to_be_filled, TICKERS_COLUMN_INDEX, ticker)
        self.spreadsheet.update_cell(next_row_to_be_filled, VALUE_COLUMN_INDEX, fii_data['value'] if fii_data['value'] != '' else 0)
        self.spreadsheet.update_cell(next_row_to_be_filled, DATE_COLUMN_INDEX, fii_data['date'])
        # TODO: validate limit and next available row to be filled
        next_row_to_be_filled += 1

class WebscrapingWorkflow(Workflow):
    def __init__(self, mode, spreadsheet):
      super().__init__(mode, spreadsheet)

    def validate_input(self, fiis_data: dict):
      # validate that at least one FII has data extracted
      return True if [f for f in fiis_data.keys() if fiis_data[f]['value'] != ''] else False

    def check_spreadsheet_state(self):
      return super().check_spreadsheet_state()


class CollectedDataWorkflow(Workflow):
    def __init__(self, mode, spreadsheet):
      super().__init__(mode, spreadsheet)

    def validate_input(self, fiis_data: dict):
      super().validate_input(fiis_data)
      missing_fiis = [fii for fii in fiis_data.keys() if fii not in self.original_fiis_list]
      if len(missing_fiis) == len(fiis_data.keys()):
        logging.warning('No FIIs from the collected data are registered in the spreadsheet.')
        return False
      elif not all(fii in self.original_fiis_list for fii in fiis_data.keys()):
        logging.info(f'The following FIIs are not registered in the spreadsheet: {missing_fiis}')
      return self._validate_values(fiis_data)

    def check_spreadsheet_state(self):
      return super().check_spreadsheet_state()

    def _validate_values(self, fiis_data: dict):
      validated_fiis = {}
      is_valid = False
      for fii_code, fii_info in fiis_data.items():
        value = fii_info.get('value', '')
        date = fii_info.get('date', '')
        if not value or not date:
          logging.warning(f'FII {fii_code} has missing value or date: value="{value}", date="{date}"')
        # check if values are in correct currency format X,XX
        elif not re.match(r'^\d+,\d+$', value):
          logging.warning(f'FII {fii_code} has invalid value format: "{value}". Expected format is numeric with comma as decimal separator, e.g., "1,23"')
        # check if date is in correct format DD/MM/YYYY
        elif not re.match(r'^\d{2}/\d{2}/\d{4}$', date):
          logging.warning(f'FII {fii_code} has invalid date format: "{date}". Expected format is "DD/MM/YYYY"')
        # check if date is no longer than one month ago
        elif datetime.strptime(date, '%d/%m/%Y') < datetime.now() - timedelta(days=DAYS_LIMIT):
          logging.warning(f'FII {fii_code} has a date older than {DAYS_LIMIT} days: "{date}"')
        else:
          is_valid = True
          validated_fiis[fii_code] = fii_info
      return is_valid, validated_fiis

    def register_fiis(self, fiis_data: dict, fiis_registered: list, next_row_to_be_filled=None):
      """
        Arguments:
          fiis_data: records to be saved
          registered_fiis: records tuple (index, fii) already saved to compare whether to save or not
      """
      if isinstance(fiis_data, dict):
        fiis = fiis_data
      else:
        fiis = json.loads(fiis_data)
      logging.info(f'\nProcessing {len(fiis)} FIIs')
      for ticker, fii_data in fiis.items():
        logging.info(f'FII: {ticker} => R$ {fii_data["value"]} em {fii_data["date"]}')
        # check if FII is already registered but has zero value filled
        if ticker in [fii for index,fii in fiis_registered]:
          # fii_index = [index for index,fii in fiis_registered].index(ticker)
          fii_index = next((index for index,c in fiis_registered if c == ticker), None)
          fii_row_in_spreadsheet = fii_index + (HEADER_ROW + 1)
          existing_value = self.spreadsheet.get_cell_value(fii_row_in_spreadsheet, VALUE_COLUMN_INDEX)
          if existing_value == '' or existing_value == 'R$ 0,00':
            logging.info(f'FII {ticker} already registered with value {existing_value}: needs to be overwritten')
            next_row_to_be_filled = fii_row_in_spreadsheet
        self.spreadsheet.update_cell(next_row_to_be_filled, TICKERS_COLUMN_INDEX, ticker)
        self.spreadsheet.update_cell(next_row_to_be_filled, VALUE_COLUMN_INDEX, fii_data['value'] if fii_data['value'] != '' else 0)
        self.spreadsheet.update_cell(next_row_to_be_filled, DATE_COLUMN_INDEX, fii_data['date'])
        # TODO: validate limit and next available row to be filled
        next_row_to_be_filled += 1



class CrewAIWorkflow(CollectedDataWorkflow):
    def __init__(self, mode, spreadsheet):
      super().__init__(mode, spreadsheet)
      self.website_url = CREWAI_CONFIG.get('website_url', 'https://investidor10.com.br/fiis/')
      self.model = CREWAI_CONFIG.get('model', 'perplexity/sonar')
      self.max_rpm = CREWAI_CONFIG.get('max_rpm', 10)

    def validate_input(self, fiis_data=None):
      """Load FII master list from spreadsheet. No external input needed."""
      Workflow.validate_input(self, fiis_data or {})
      return True, {}

    def search_dividends(self, fiis_to_search: list) -> dict:
      """
      Run CrewAI agents to discover dividend data for the given FIIs.

      Args:
          fiis_to_search: List of FII ticker codes to search for

      Returns:
          Dictionary mapping FII codes to their dividend data
      """
      from tools.crewai_search import search_fii_dividends
      return search_fii_dividends(fiis_to_search, self.website_url, self.model, self.max_rpm)


def setup_spreadsheet(spreadsheet_id: str, credentials_path: str) -> SpreadsheetIntegration:
    return SpreadsheetIntegration(spreadsheet_id, cred_file_path=credentials_path)

def setup_workflow(mode: str, spreadsheet: SpreadsheetIntegration):
    if mode == 'webscraping':
        return WebscrapingWorkflow(mode, spreadsheet)
    elif mode == 'collected':
        return CollectedDataWorkflow(mode, spreadsheet)
    elif mode == 'crewai':
        return CrewAIWorkflow(mode, spreadsheet)
    else:
        raise ValueError('Invalid mode selected. Choose "webscraping", "collected" or "crewai".')

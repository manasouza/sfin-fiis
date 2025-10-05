import gspread
import json
from google.oauth2.service_account import Credentials
from gspread.worksheet import Worksheet

DEFAULT_SHEETS_API_SCOPE = ['https://spreadsheets.google.com/feeds',
                            'https://www.googleapis.com/auth/drive']

def _get_json_auth_key(local=False, local_path=None, remote_function=None):
    if local:
        if local_path == '':
            raise ValueError('for local option, local file path is required')
        print('using local creds file')
        with open(local_path, 'r') as local_json:
            return json.loads(local_json.read())
    else:
        print('using remote function creds file')
        return json.loads(remote_function())


class SpreadsheetIntegration:

    main_worksheet = None

    def __init__(self, spreadsheet_id: str, cred_file_path=None, cred_remote_function=None, scope=DEFAULT_SHEETS_API_SCOPE):
        self.spreadsheet_id = spreadsheet_id
        local_usage = True if cred_file_path else False
        self.creds_key_json = _get_json_auth_key(local=local_usage, local_path=cred_file_path, remote_function=cred_remote_function)
        credentials = Credentials.from_service_account_info(
            self.creds_key_json,
            scopes=scope
        )
        self.set_credentials(credentials)

    def set_credentials(self, credentials):
        self.gc = gspread.authorize(credentials)

    def set_worksheet(self, worksheet_name: str):
        self.worksheet = self.gc.open_by_key(self.spreadsheet_id).worksheet(worksheet_name)

    def format(self, cell_range: str, format_structure: dict):
        self.worksheet.format(cell_range, format_structure)

    def find(self, query, from_row=0):
        import ipdb; ipdb.set_trace()
        return self.worksheet.find(query, in_row=from_row)

    def get_cells_in_the_range(self, first_row, first_column, last_row, last_column):
        return self.worksheet.range(first_row, first_column, last_row, last_column)

    def get_column_values(self, column_index):
        return self.worksheet.col_values(column_index)

    def insert_rows(self, values, from_row=1, mode='RAW'):
        try:
            self.worksheet.insert_rows(['' for c in values], from_row, value_input_option=mode)
        except gspread.exceptions.APIError as e:
            print('Error while inserting rows, altough rows inserted')

    def update_cell(self, row, column, value):
        self.worksheet.update_cell(row, column, value)

    def fii_processed(self, fii_code, ref_row, last_row, ticker_column):
        return len([fii_code for ticker in self.worksheet.range(ref_row, ticker_column, last_row, ticker_column) if ticker.value == fii_code]) > 0

    def _next_available_row(self, worksheet: Worksheet):
        str_list = list(filter(None, worksheet.col_values(1)))
        return str(len(str_list)+1)
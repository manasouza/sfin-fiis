import os
import argparse
import sys

from fiis_workflow import setup_workflow, setup_spreadsheet
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', required=True, default='collected', help='mode to run the script, either "webscraping" or "collected"')
    parser.add_argument('-f', '--fiis', required=True, default='{}', help='FIIs to process in JSON format, e.g. \'{"HGLG11": {"value":"R$ 1,00","date":"2025-06-05"}}\'')
    # parser.add_argument('arg', nargs='*')
    args = parser.parse_args()

    spreadsheet = setup_spreadsheet(os.getenv('SPREADSHEET_ID'), os.getenv('CREDENTIALS_PATH'))

    workflow = setup_workflow(args.mode, spreadsheet)
    result = workflow.validate_input(eval(args.fiis))
    if not result:
        print('Input validation failed. Exiting.')
        return
    starting_point, next_row_to_be_filled, fiis_valid, fiis_registered = workflow.check_spreadsheet_state()
    workflow.register_fiis(args.fiis, [f for f in fiis_registered if f not in fiis_valid], next_row_to_be_filled=next_row_to_be_filled)
    # check_dividend_yield(argv, mode='collected', fiis_collected={'HGLG11': {'value':'','date':''}})


if __name__ == "__main__":
    main()
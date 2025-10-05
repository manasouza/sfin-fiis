import os
import sys
from fiis import check_dividend_yield, setup_spreadsheet
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def main(argv):
    setup_spreadsheet(os.getenv('SPREADSHEET_ID'), os.getenv('CREDENTIALS_PATH'))
    # check_dividend_yield(argv)
    check_dividend_yield(argv, mode='collected', fiis_collected=['HGLG11', 'KNRI11', 'VISC11'])

if __name__ == "__main__":
    main(sys.argv[1:])
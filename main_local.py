import os
import sys
from fiis import check_dividend_yield, setup_spreadsheet

def main(argv):
    setup_spreadsheet(os.getenv('SPREADSHEET_ID'), os.getenv('CREDENTIALS_PATH'))
    check_dividend_yield(argv)

if __name__ == "__main__":
    main(sys.argv[1:])
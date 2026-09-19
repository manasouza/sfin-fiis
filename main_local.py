import os
import json
import logging
import datetime
import argparse
import sys

from fiis_workflow import setup_workflow, setup_spreadsheet
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def configure_logging():
    log_dir = os.getenv('LOG_DIR', '/var/log')
    today = datetime.date.today().strftime('%Y%m%d')
    logfile = os.path.join(log_dir, f'sfinfiis-{today}.log')

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (fallbacks to cwd if /var/log not writable)
    try:
        file_handler = logging.FileHandler(logfile, encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except (PermissionError, OSError):
        fallback_log = os.path.join(os.getcwd(), f'sfinfiis-{today}.log')
        file_handler = logging.FileHandler(fallback_log, encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Redirect stdout/stderr to logging
    class StreamToLogger(object):
        def __init__(self, logger, level):
            self.logger = logger
            self.level = level

        def write(self, buf):
            if buf and not buf.isspace():
                for line in buf.rstrip().splitlines():
                    self.logger.log(self.level, line)

        def flush(self):
            pass

    sys.stdout = StreamToLogger(root_logger, logging.INFO)
    sys.stderr = StreamToLogger(root_logger, logging.ERROR)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', required=True, choices=['webscraping', 'collected', 'crewai'],
                        help='mode to run the script: "webscraping", "collected" or "crewai"')
    parser.add_argument('-f', '--fiis', default='{}',
                        help='FIIs to process in JSON format (required for "collected" mode)')
    args = parser.parse_args()

    spreadsheet = setup_spreadsheet(os.getenv('SPREADSHEET_ID'), os.getenv('CREDENTIALS_PATH'))

    workflow = setup_workflow(args.mode, spreadsheet)

    if args.mode == 'crewai':
        workflow.validate_input()
        starting_point, next_row_to_be_filled, fiis_valid, fiis_registered = workflow.check_spreadsheet_state()
        fiis_to_search = [fii for fii in workflow.original_fiis_list if fii not in fiis_valid]
        if not fiis_to_search:
            logging.info('No FIIs registered in the spreadsheet.')
            return
        logging.info(f'FIIs to search with CrewAI: {fiis_to_search}')
        search_results = workflow.search_dividends(fiis_to_search)
        is_valid, validated_fiis = workflow._validate_values(search_results)
        if not is_valid:
            logging.warning('No valid results from CrewAI search.')
            return
        workflow.register_fiis(validated_fiis, fiis_registered, next_row_to_be_filled=next_row_to_be_filled)
    elif args.mode == 'collected':
        fiis_data = json.loads(args.fiis)
        result, validated_fiis = workflow.validate_input(fiis_data)
        if not result:
            print('Input validation failed. Exiting.')
            return
        starting_point, next_row_to_be_filled, fiis_valid, fiis_registered = workflow.check_spreadsheet_state()
        fiis_pending = [(index, fii) for index, fii in fiis_registered if fii not in fiis_valid]
        workflow.register_fiis(validated_fiis, fiis_pending, next_row_to_be_filled=next_row_to_be_filled)
    elif args.mode == 'webscraping':
        workflow.validate_input()
        starting_point, next_row_to_be_filled, fiis_valid, fiis_registered = workflow.check_spreadsheet_state()
        fiis_to_search = [fii for fii in workflow.original_fiis_list if fii not in fiis_valid]
        if not fiis_to_search:
            logging.info('No FIIs registered in the spreadsheet.')
            return
        logging.info(f'FIIs to search with webscraping: {fiis_to_search}')
        search_results = workflow.search_dividends(fiis_to_search)
        is_valid, validated_fiis = workflow._validate_values(search_results)
        if not is_valid:
            logging.warning('No valid results from webscraping search.')
            return
        workflow.register_fiis(validated_fiis, fiis_registered, next_row_to_be_filled=next_row_to_be_filled)
    else:
        logging.error('Invalid mode selected: {}. Exiting.'.format(args.mode))
        sys.exit(1)


if __name__ == "__main__":
    configure_logging()
    main()

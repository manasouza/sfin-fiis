import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch


def install_dependency_stubs():
    gspread_stub = types.ModuleType("gspread")
    gspread_stub.authorize = lambda credentials: None
    gspread_stub.exceptions = types.SimpleNamespace(APIError=Exception)
    gspread_stub.worksheet = types.SimpleNamespace(Worksheet=object)
    sys.modules["gspread"] = gspread_stub

    gspread_utils_stub = types.ModuleType("gspread.utils")
    gspread_utils_stub.rowcol_to_a1 = lambda row, col: f"{col}{row}"
    gspread_utils_stub.ValueRenderOption = types.SimpleNamespace(formula="formula")
    sys.modules["gspread.utils"] = gspread_utils_stub

    gspread_worksheet_stub = types.ModuleType("gspread.worksheet")
    gspread_worksheet_stub.Worksheet = object
    sys.modules["gspread.worksheet"] = gspread_worksheet_stub

    google_module = types.ModuleType("google")
    google_oauth_module = types.ModuleType("google.oauth2")
    google_service_account_module = types.ModuleType("google.oauth2.service_account")

    class Credentials:
        @classmethod
        def from_service_account_info(cls, *args, **kwargs):
            return cls()

    google_service_account_module.Credentials = Credentials
    google_module.oauth2 = google_oauth_module
    google_oauth_module.service_account = google_service_account_module
    sys.modules["google"] = google_module
    sys.modules["google.oauth2"] = google_oauth_module
    sys.modules["google.oauth2.service_account"] = google_service_account_module


install_dependency_stubs()

sys.path.insert(0, "/home/manasouza/development/git/sfin-fiis")

from fiis_workflow import CollectedDataWorkflow, Workflow  # noqa: E402


class FakeCell:
    def __init__(self, value=""):
        self.value = value


class FakeSpreadsheet:
    def __init__(self):
        self.worksheet_name = None
        self.calls = []
        self.row_values = {}

    def set_worksheet(self, worksheet_name):
        self.worksheet_name = worksheet_name

    def get_column_values(self, column_index):
        return ["ABC11", "XPML11", "XFIR11"]

    def find(self, query, from_row=0):
        return types.SimpleNamespace(col=2, row=2)

    def get_cells_in_the_range(self, first_row, first_column, last_row, last_column):
        if first_column == 2:
            return [FakeCell("XPML11"), FakeCell("XFIR11")]
        if first_column == 5:
            return [FakeCell("R$ 0,00"), FakeCell("R$ 1,23")]
        return []

    def copy_cells(self, cell_range):
        self.calls.append(("copy_cells", cell_range))
        return [["copied"]]

    def paste_cells(self, cell_range, values):
        self.calls.append(("paste_cells", cell_range, values))

    def insert_rows(self, values, from_row=1, mode="RAW"):
        self.calls.append(("insert_rows", values, from_row, mode))

    def update_cell(self, row, column, value, from_range_row=None, to_range_row=None, from_range_column=None, to_range_column=None):
        self.calls.append(("update_cell", row, column, value, from_range_row, to_range_row))
        self.row_values[(row, column)] = value

    def format(self, cell_range, format_structure):
        self.calls.append(("format", cell_range, format_structure))

    def get_cell_value(self, row, column):
        return self.row_values.get((row, column), "")


class TestWorkflowValidation(unittest.TestCase):
    def setUp(self):
        self.spreadsheet = FakeSpreadsheet()
        self.workflow = CollectedDataWorkflow("collected", self.spreadsheet)
        self.workflow.original_fiis_list = ["XPML11", "XFIR11", "KNCA11"]

    def test_validate_input_collects_registered_tickers(self):
        workflow = Workflow("collected", self.spreadsheet)
        workflow.validate_input({})
        self.assertEqual(workflow.original_fiis_list, ["XPML11", "XFIR11"])

    @patch("fiis_workflow.DAYS_LIMIT", 30)
    def test_validate_values_accepts_recent_valid_data(self):
        recent_date = (datetime.now() - timedelta(days=5)).strftime("%d/%m/%Y")
        fiis_data = {"XPML11": {"value": "12,34", "date": recent_date}}

        is_valid, validated = self.workflow._validate_values(fiis_data)

        self.assertTrue(is_valid)
        self.assertIn("XPML11", validated)

    @patch("fiis_workflow.DAYS_LIMIT", 30)
    def test_validate_values_rejects_old_dates(self):
        old_date = (datetime.now() - timedelta(days=31)).strftime("%d/%m/%Y")
        fiis_data = {"XPML11": {"value": "12,34", "date": old_date}}

        is_valid, validated = self.workflow._validate_values(fiis_data)

        self.assertFalse(is_valid)
        self.assertNotIn("XPML11", validated)

    def test_validate_values_rejects_missing_fields(self):
        fiis_data = {"XPML11": {"value": "", "date": "10/10/2025"}}

        is_valid, validated = self.workflow._validate_values(fiis_data)

        self.assertFalse(is_valid)
        self.assertNotIn("XPML11", validated)

    def test_collected_validate_input_returns_false_when_all_fiis_are_unregistered(self):
        result, validated = self.workflow.validate_input({"ABC11": {"value": "1,23", "date": "10/10/2025"}})

        self.assertFalse(result)
        self.assertEqual(validated, {})

    def test_collected_validate_input_keeps_registered_fiis(self):
        result, validated = self.workflow.validate_input({"XPML11": {"value": "1,23", "date": "10/10/2025"}})

        self.assertTrue(result)
        self.assertIn("XPML11", validated)


class TestSpreadsheetState(unittest.TestCase):
    def setUp(self):
        self.spreadsheet = FakeSpreadsheet()
        self.workflow = CollectedDataWorkflow("collected", self.spreadsheet)
        self.workflow.original_fiis_list = ["XPML11", "XFIR11"]

    def test_check_spreadsheet_state_returns_next_row_and_registered_fiis(self):
        starting_point, next_row_to_be_filled, fiis_valid, fiis_registered = self.workflow.check_spreadsheet_state()

        self.assertEqual(starting_point, 3)
        self.assertEqual(next_row_to_be_filled, 5)
        self.assertEqual(fiis_valid, ["XPML11", "XFIR11"])
        self.assertEqual(fiis_registered, [(0, "XPML11"), (1, "XFIR11")])


class TestRegistration(unittest.TestCase):
    def setUp(self):
        self.spreadsheet = FakeSpreadsheet()
        self.workflow = CollectedDataWorkflow("collected", self.spreadsheet)
        self.workflow.original_fiis_list = ["XPML11", "XFIR11"]
        self.spreadsheet.row_values[(3, 5)] = "R$ 0,00"

    def test_register_fiis_overwrites_existing_zero_value_rows(self):
        fiis_data = {"XPML11": {"value": "5,00", "date": "10/10/2025"}}
        fiis_registered = [(0, "XPML11")]

        self.workflow.register_fiis(fiis_data, fiis_registered, next_row_to_be_filled=3)

        self.assertEqual(self.spreadsheet.row_values[(3, 2)], "XPML11")
        self.assertEqual(self.spreadsheet.row_values[(3, 5)], "5,00")
        self.assertEqual(self.spreadsheet.row_values[(3, 1)], "10/10/2025")


if __name__ == "__main__":
    unittest.main()
import sys
import types
import unittest
from unittest.mock import Mock


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

from fiis_workflow import CollectedDataWorkflow, CrewAIWorkflow, WebscrapingWorkflow, setup_workflow  # noqa: E402


class TestWorkflowFactory(unittest.TestCase):
    def test_setup_workflow_returns_expected_instances(self):
        spreadsheet = Mock()

        self.assertIsInstance(setup_workflow("webscraping", spreadsheet), WebscrapingWorkflow)
        self.assertIsInstance(setup_workflow("collected", spreadsheet), CollectedDataWorkflow)
        self.assertIsInstance(setup_workflow("crewai", spreadsheet), CrewAIWorkflow)

    def test_setup_workflow_rejects_invalid_mode(self):
        with self.assertRaises(ValueError):
            setup_workflow("unsupported", Mock())

    def test_webscraping_validate_input_without_data_returns_empty_payload(self):
        spreadsheet = Mock()
        spreadsheet.get_column_values.return_value = ["XPML11", "XFIR11"]
        workflow = WebscrapingWorkflow("webscraping", spreadsheet)

        result, payload = workflow.validate_input()

        self.assertTrue(result)
        self.assertEqual(payload, {})


if __name__ == "__main__":
    unittest.main()

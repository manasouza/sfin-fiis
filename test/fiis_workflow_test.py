import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fiis_workflow import CollectedDataWorkflow

DATE_FORMAT = '%d-%m-%Y'

class TestValidateValues(unittest.TestCase):
    """Test suite for CollectedDataWorkflow._validate_values method"""
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        self.mock_spreadsheet = Mock()
        self.workflow = CollectedDataWorkflow('collected', self.mock_spreadsheet)
        self.workflow.original_fiis_list = ['XPML11', 'XFIR11', 'KNCA11']
    
    def test_valid_fii_data(self):
        """Test validation with completely valid FII data"""
        fiis_data = {
            'XPML11': {
                'value': '23,45',
                'date': '11/10/2025'
            }
        }        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        self.assertTrue(is_valid)
        self.assertIn('XPML11', validated)
        self.assertEqual(validated['XPML11']['value'], '23,45')
    
    def test_valid_multiple_fiis(self):
        """Test validation with multiple valid FII records"""
        fiis_data = {
            'XPML11': {
                'value': '0,45',
                'date': '10/11/2025'
            },
            'XFIR11': {
                'value': '4,78',
                'date': '2025-11-04'
            }
        }        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        self.assertTrue(is_valid)
        self.assertEqual(len(validated), 1)
        self.assertIn('XPML11', validated)
        self.assertIn('XFIR11', validated)
    
    def test_missing_value_field(self):
        """Test validation with missing value field"""
        fiis_data = {
            'XPML11': {
                'value': '',
                'date': '2025-11-10'
            }
        }        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        self.assertFalse(is_valid)
        self.assertNotIn('XPML11', validated)
    
    def test_missing_date_field(self):
        """Test validation with missing date field"""
        fiis_data = {
            'XPML11': {
                'value': '123,45',
                'date': ''
            }
        }        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        assert is_valid is False
        assert 'XPML11' not in validated
    
    def test_invalid_value_format(self):
        """Test validation with invalid currency format"""
        fiis_data = {
            'XPML11': {
                'value': '123.45',  # Wrong format, should be X,XX
                'date': '15/01/2025'
            }
        }
        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        assert is_valid is False
        assert 'XPML11' not in validated
    
    def test_invalid_date_format(self):
        """Test validation with invalid date format"""
        fiis_data = {
            'XPML11': {
                'value': '123,45',
                'date': '15/01/2024'  # Wrong format, should be YYYY-MM-DD
            }
        }
        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        assert is_valid is False
        assert 'XPML11' not in validated
    
    @patch('fiis_workflow.DAYS_LIMIT', 30)
    def test_date_too_old(self):
        """Test validation with date older than DAYS_LIMIT"""
        old_date = (datetime.now() - timedelta(days=31)).strftime(DATE_FORMAT)
        fiis_data = {
            'XPML11': {
                'value': '123,45',
                'date': old_date
            }
        }
        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        assert is_valid is False
        assert 'XPML11' not in validated
    
    @patch('fiis_workflow.DAYS_LIMIT', 30)
    def test_date_within_limit(self):
        """Test validation with date within DAYS_LIMIT"""
        recent_date = (datetime.now() - timedelta(days=15)).strftime(DATE_FORMAT)
        fiis_data = {
            'XPML11': {
                'value': '123,45',
                'date': recent_date
            }
        }
        
        is_valid, validated = self.workflow._validate_values(fiis_data)
        assert is_valid is True
        assert 'XPML11' in validated
    
    # def test_multiple_fiis_mixed_validation(self):
    #     """Test validation with multiple FIIs, some valid and some invalid"""
    #     fiis_data = {
    #         'XPML11': {
    #             'value': '123,45',
    #             'date': '2024-01-15'
    #         },
    #         'XFIR11': {
    #             'value': '100,00',
    #             'date': '2024-01-16'
    #         },
    #         'KNCA11': {
    #             'value': 'invalid',
    #             'date': '2024-01-17'
    #         }
    #     }
        
    #     is_valid, validated = self.workflow._validate_values(fiis_data)
    #     assert is_valid is True
    #     assert len(validated) == 2
    #     assert 'XPML11' in validated
    #     assert 'XFIR11' in validated
    #     assert 'KNCA11' not in validated
    
    # def test_value_with_single_decimal(self):
    #     """Test validation rejects values with only one decimal place"""
    #     fiis_data = {
    #         'XPML11': {
    #             'value': '123,4',  # Only one decimal
    #             'date': '2024-01-15'
    #         }
    #     }
        
    #     is_valid, validated = self.workflow._validate_values(fiis_data)
    #     assert is_valid is True
    #     assert 'XPML11' not in validated

if __name__ == "__main__":
    unittest.main()
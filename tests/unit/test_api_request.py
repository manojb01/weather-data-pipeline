"""Unit tests for API request module."""

import pytest
from unittest.mock import patch, Mock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src/pipelines'))

from api_request import fetch_data, mock_fetch_data


class TestAPIRequest:
    """Test cases for API request functions."""

    def test_mock_fetch_data_returns_dict(self):
        """Test that mock_fetch_data returns a dictionary."""
        result = mock_fetch_data()
        assert isinstance(result, dict)
        assert 'location' in result
        assert 'current' in result

    def test_mock_fetch_data_has_required_fields(self):
        """Test that mock data has all required fields."""
        result = mock_fetch_data()

        # Check location fields
        assert 'name' in result['location']
        assert 'localtime' in result['location']
        assert 'utc_offset' in result['location']

        # Check current weather fields
        assert 'temperature' in result['current']
        assert 'weather_descriptions' in result['current']
        assert 'wind_speed' in result['current']

    @patch('api_request.requests.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful API fetch."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            'location': {'name': 'New York'},
            'current': {'temperature': 20}
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Test
        result = fetch_data('http://test.com')

        assert result is not None
        assert 'location' in result
        mock_get.assert_called_once_with('http://test.com')

    @patch('api_request.requests.get')
    def test_fetch_data_failure(self, mock_get):
        """Test API fetch failure handling."""
        # Mock failed response
        mock_get.side_effect = Exception("API Error")

        # Test
        with pytest.raises(Exception):
            fetch_data('http://test.com')


if __name__ == '__main__':
    pytest.main([__file__])

"""Unit tests for insert records module."""

import pytest
from unittest.mock import patch, Mock, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src/pipelines'))


class TestInsertRecords:
    """Test cases for database insertion functions."""

    @patch('psycopg2.connect')
    def test_connect_to_db_success(self, mock_connect):
        """Test successful database connection."""
        from insert_records import connect_to_db

        # Mock connection
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        # Test
        result = connect_to_db()

        assert result is not None
        mock_connect.assert_called_once()

    @patch('psycopg2.connect')
    def test_connect_to_db_failure(self, mock_connect):
        """Test database connection failure."""
        from insert_records import connect_to_db
        import psycopg2

        # Mock connection failure
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        # Test
        with pytest.raises(psycopg2.Error):
            connect_to_db()

    def test_create_table(self):
        """Test table creation."""
        from insert_records import create_table

        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Test
        create_table(mock_conn)

        # Verify execute was called
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_insert_records(self):
        """Test data insertion."""
        from insert_records import insert_records

        # Mock connection and cursor
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Test data
        test_data = {
            'location': {
                'name': 'New York',
                'localtime': '2025-07-11 12:53',
                'utc_offset': '-4.0'
            },
            'current': {
                'temperature': 27,
                'weather_descriptions': ['Overcast'],
                'wind_speed': 15
            }
        }

        # Test
        insert_records(mock_conn, test_data)

        # Verify execute was called
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__])

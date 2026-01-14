"""Integration tests for the complete data pipeline."""

import pytest
import psycopg2
from unittest.mock import patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src/pipelines'))


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the data pipeline."""

    @pytest.fixture
    def db_connection(self):
        """Create a test database connection."""
        conn = psycopg2.connect(
            host='localhost',
            port=5001,
            dbname='db',
            user='postgres',
            password='postgres'
        )
        yield conn
        conn.close()

    @pytest.mark.skip(reason="Requires running database")
    def test_database_connection(self, db_connection):
        """Test database connectivity."""
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1

    @pytest.mark.skip(reason="Requires running database")
    def test_table_exists(self, db_connection):
        """Test that raw_weather_data table exists."""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'dev'
                AND table_name = 'raw_weather_data'
            );
        """)
        result = cursor.fetchone()
        assert result[0] is True

    @patch('api_request.requests.get')
    @pytest.mark.skip(reason="Requires running database")
    def test_full_pipeline(self, mock_get, db_connection):
        """Test the complete pipeline from API to database."""
        from insert_records import main
        from api_request import mock_fetch_data

        # Mock API response
        mock_response = mock_fetch_data()

        # Run pipeline
        main()

        # Verify data in database
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM dev.raw_weather_data
            WHERE city = 'New York'
        """)
        result = cursor.fetchone()
        assert result[0] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json
import httpx
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from database import (
    sanitize_sql_value,
    make_snowflake_request,
    execute_snowflake_query,
    format_snowflake_row,
    parse_snowflake_timestamp,
    get_issue_labels,
    get_issue_comments,
    get_issue_links
)


class TestSanitizeSqlValue:
    """Test cases for sanitize_sql_value function"""

    def test_sanitize_string_with_quotes(self):
        """Test sanitizing string with single quotes"""
        result = sanitize_sql_value("test'value")
        assert result == "test''value"

    def test_sanitize_string_multiple_quotes(self):
        """Test sanitizing string with multiple single quotes"""
        result = sanitize_sql_value("test'val'ue'")
        assert result == "test''val''ue''"

    def test_sanitize_string_no_quotes(self):
        """Test sanitizing string without quotes"""
        result = sanitize_sql_value("test_value")
        assert result == "test_value"

    def test_sanitize_non_string(self):
        """Test sanitizing non-string values"""
        result = sanitize_sql_value(123)
        assert result == "123"

    def test_sanitize_empty_string(self):
        """Test sanitizing empty string"""
        result = sanitize_sql_value("")
        assert result == ""


class TestMakeSnowflakeRequest:
    """Test cases for make_snowflake_request function"""

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'default_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_successful_post_request(self, mock_client_class):
        """Test successful POST request"""
        # Create mock response
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()
        
        # Create mock client instance
        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        
        # Set up the client class to return our mock instance
        mock_client_class.return_value = mock_client_instance
        
        result = await make_snowflake_request("endpoint", "POST", {"test": "data"})
        
        assert result == {"data": []}
        mock_client_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', None)
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    async def test_missing_token(self):
        """Test request with missing token"""
        result = await make_snowflake_request("endpoint", "POST", {"test": "data"})
        assert result is None

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'default_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_provided_token_override(self, mock_client_class):
        """Test that provided token overrides default"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()
        
        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance
        
        await make_snowflake_request("endpoint", "POST", {"test": "data"}, "custom_token")
        
        # Verify custom token was used
        args, kwargs = mock_client_instance.request.call_args
        headers = kwargs['headers']
        assert headers['Authorization'] == 'Bearer custom_token'

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_get_request(self, mock_client_class):
        """Test GET request with params"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()
        
        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance
        
        await make_snowflake_request("endpoint", "GET", {"param": "value"})
        
        # Verify params were used instead of json
        args, kwargs = mock_client_instance.request.call_args
        assert 'params' in kwargs
        assert 'json' not in kwargs

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_json_decode_error(self, mock_client_class):
        """Test handling of JSON decode error"""
        mock_response = MagicMock()
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_response.text = "Invalid response"
        mock_response.raise_for_status = MagicMock()
        
        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance
        
        result = await make_snowflake_request("endpoint", "POST", {"test": "data"})
        assert result is None

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_http_error(self, mock_client_class):
        """Test handling of HTTP error"""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        
        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(side_effect=httpx.HTTPStatusError("400", request=None, response=mock_response))
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance
        
        result = await make_snowflake_request("endpoint", "POST", {"test": "data"})
        assert result is None


class TestExecuteSnowflakeQuery:
    """Test cases for execute_snowflake_query function"""

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_successful_query(self, mock_track, mock_request):
        """Test successful query execution"""
        mock_request.return_value = {
            "data": [["row1col1", "row1col2"], ["row2col1", "row2col2"]]
        }
        
        result = await execute_snowflake_query("SELECT * FROM test", "token")
        
        assert len(result) == 2
        assert result[0] == ["row1col1", "row1col2"]
        mock_track.assert_called_once()

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_with_pagination(self, mock_track, mock_request):
        """Test query execution with pagination"""
        # First call returns data with partition info
        first_response = {
            "data": [["row1col1", "row1col2"]],
            "statementHandle": "handle123",
            "resultSetMetaData": {
                "partitionInfo": [{"startRow": 0}, {"startRow": 1}]
            }
        }
        
        # Second call returns additional partition data
        second_response = {
            "data": [["row2col1", "row2col2"]]
        }
        
        mock_request.side_effect = [first_response, second_response]
        
        result = await execute_snowflake_query("SELECT * FROM test", "token")
        
        assert len(result) == 2
        assert result[0] == ["row1col1", "row1col2"]
        assert result[1] == ["row2col1", "row2col2"]
        assert mock_request.call_count == 2

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_resultset_format(self, mock_track, mock_request):
        """Test query execution with resultSet format"""
        mock_request.return_value = {
            "resultSet": {
                "data": [["row1col1", "row1col2"]]
            }
        }
        
        result = await execute_snowflake_query("SELECT * FROM test", "token")
        
        assert len(result) == 1
        assert result[0] == ["row1col1", "row1col2"]

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_no_response(self, mock_track, mock_request):
        """Test query execution when no response is returned"""
        mock_request.return_value = None
        
        result = await execute_snowflake_query("SELECT * FROM test", "token")
        
        assert result == []
        mock_track.assert_called_once()

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_exception(self, mock_track, mock_request):
        """Test query execution when exception occurs"""
        mock_request.side_effect = Exception("Database error")
        
        result = await execute_snowflake_query("SELECT * FROM test", "token")
        
        assert result == []
        mock_track.assert_called_once()


class TestParseSnowflakeTimestamp:
    """Test cases for parse_snowflake_timestamp function"""

    def test_parse_timestamp_with_offset(self):
        """Test parsing timestamp with timezone offset"""
        # Test with actual KONFLUX-9430 timestamp
        timestamp_str = "1753767533.658000000 1440"
        result = parse_snowflake_timestamp(timestamp_str)
        
        # Should convert to ISO format with timezone offset applied
        assert result == "2025-07-30T05:38:53.658000+00:00"

    def test_parse_timestamp_different_offsets(self):
        """Test parsing timestamps with different timezone offsets"""
        test_cases = [
            ("1753767533.658000000 1440", "2025-07-30T05:38:53.658000+00:00"),  # +24 hours
            ("1753767533.658000000 0", "2025-07-29T05:38:53.658000+00:00"),     # no offset
            ("1753767533.658000000 -300", "2025-07-29T00:38:53.658000+00:00"),  # -5 hours
        ]
        
        for input_timestamp, expected_output in test_cases:
            result = parse_snowflake_timestamp(input_timestamp)
            assert result == expected_output

    def test_parse_timestamp_without_offset(self):
        """Test parsing timestamp without timezone offset"""
        timestamp_str = "1753767533.658000000"
        result = parse_snowflake_timestamp(timestamp_str)
        
        # Should convert to UTC ISO format
        assert result == "2025-07-29T05:38:53.658000+00:00"

    def test_parse_timestamp_integer_seconds(self):
        """Test parsing timestamp with integer seconds"""
        timestamp_str = "1753767533 1440"
        result = parse_snowflake_timestamp(timestamp_str)
        
        # Should handle integer timestamps
        assert result == "2025-07-30T05:38:53+00:00"

    def test_parse_timestamp_none_input(self):
        """Test parsing None input"""
        result = parse_snowflake_timestamp(None)
        assert result is None

    def test_parse_timestamp_empty_string(self):
        """Test parsing empty string"""
        result = parse_snowflake_timestamp("")
        assert result == ""

    def test_parse_timestamp_non_string_input(self):
        """Test parsing non-string input"""
        result = parse_snowflake_timestamp(123)
        assert result == 123

    def test_parse_timestamp_invalid_format(self):
        """Test parsing invalid timestamp format"""
        invalid_timestamps = [
            "invalid_timestamp",
            "1753767533.658000000 invalid_offset",
            "not_a_number 1440",
        ]
        
        for invalid_ts in invalid_timestamps:
            result = parse_snowflake_timestamp(invalid_ts)
            # Should return original string if parsing fails
            assert result == invalid_ts

    def test_parse_timestamp_with_extra_data(self):
        """Test parsing timestamp with extra data - should still parse the valid parts"""
        timestamp_str = "1753767533.658000000 1440 extra_data"
        result = parse_snowflake_timestamp(timestamp_str)
        
        # Function should parse the first two parts and ignore extra data
        assert result == "2025-07-30T05:38:53.658000+00:00"

    def test_parse_timestamp_edge_cases(self):
        """Test parsing edge case timestamps"""
        test_cases = [
            # Very large offset
            ("1753767533.658000000 43200", "2025-08-28T05:38:53.658000+00:00"),  # +30 days
            # Negative large offset  
            ("1753767533.658000000 -43200", "2025-06-29T05:38:53.658000+00:00"), # -30 days
            # Zero timestamp
            ("0.0 0", "1970-01-01T00:00:00+00:00"),
        ]
        
        for input_timestamp, expected_output in test_cases:
            result = parse_snowflake_timestamp(input_timestamp)
            assert result == expected_output


class TestFormatSnowflakeRow:
    """Test cases for format_snowflake_row function"""

    def test_format_matching_lengths(self):
        """Test formatting with matching row and column lengths"""
        row_data = ["value1", "value2", "value3"]
        columns = ["col1", "col2", "col3"]
        
        result = format_snowflake_row(row_data, columns)
        
        expected = {"col1": "value1", "col2": "value2", "col3": "value3"}
        assert result == expected

    def test_format_mismatched_lengths(self):
        """Test formatting with mismatched row and column lengths"""
        row_data = ["value1", "value2"]
        columns = ["col1", "col2", "col3"]
        
        result = format_snowflake_row(row_data, columns)
        
        assert result == {}

    def test_format_empty_data(self):
        """Test formatting with empty data"""
        row_data = []
        columns = []
        
        result = format_snowflake_row(row_data, columns)
        
        assert result == {}

    def test_format_with_timestamp_columns(self):
        """Test formatting with timestamp columns that should be parsed"""
        row_data = ["123", "1753767533.658000000 1440", "1753824211.261000000 1440", "regular_value"]
        columns = ["ID", "CREATED", "RESOLUTIONDATE", "SUMMARY"]
        
        result = format_snowflake_row(row_data, columns)
        
        expected = {
            "ID": "123",
            "CREATED": "2025-07-30T05:38:53.658000+00:00",
            "RESOLUTIONDATE": "2025-07-30T21:23:31.261000+00:00", 
            "SUMMARY": "regular_value"
        }
        assert result == expected

    def test_format_with_non_timestamp_columns(self):
        """Test formatting with non-timestamp columns that should not be parsed"""
        row_data = ["123", "1753767533.658000000 1440", "some_description"]
        columns = ["ID", "SOME_NUMBER", "DESCRIPTION"]  # SOME_NUMBER is not a recognized timestamp column
        
        result = format_snowflake_row(row_data, columns)
        
        expected = {
            "ID": "123",
            "SOME_NUMBER": "1753767533.658000000 1440",  # Should not be parsed
            "DESCRIPTION": "some_description"
        }
        assert result == expected

    def test_format_with_null_timestamp_values(self):
        """Test formatting with null timestamp values"""
        row_data = ["123", None, "", "value"]
        columns = ["ID", "CREATED", "UPDATED", "SUMMARY"]
        
        result = format_snowflake_row(row_data, columns)
        
        expected = {
            "ID": "123",
            "CREATED": None,  # None should remain None
            "UPDATED": "",    # Empty string should remain empty
            "SUMMARY": "value"
        }
        assert result == expected

    def test_format_case_insensitive_timestamp_columns(self):
        """Test that timestamp column detection is case insensitive"""
        row_data = ["123", "1753767533.658000000 1440", "1753824211.261000000 1440"]
        columns = ["id", "created", "Updated"]  # Mixed case
        
        result = format_snowflake_row(row_data, columns)
        
        # Should still parse timestamps regardless of case
        assert "2025-07-30T05:38:53.658000+00:00" in result["created"]
        assert "2025-07-30T21:23:31.261000+00:00" in result["Updated"]


class TestGetIssueLabels:
    """Test cases for get_issue_labels function"""

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    @patch('database.format_snowflake_row')
    async def test_get_labels_success(self, mock_format, mock_query):
        """Test successful label retrieval"""
        mock_query.return_value = [
            ["123", "bug"],
            ["123", "urgent"],
            ["456", "feature"]
        ]
        
        mock_format.side_effect = [
            {"ISSUE": "123", "LABEL": "bug"},
            {"ISSUE": "123", "LABEL": "urgent"},
            {"ISSUE": "456", "LABEL": "feature"}
        ]
        
        result = await get_issue_labels(["123", "456"], "token")
        
        expected = {
            "123": ["bug", "urgent"],
            "456": ["feature"]
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_labels_empty_input(self):
        """Test with empty issue IDs list"""
        result = await get_issue_labels([], "token")
        assert result == {}

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_get_labels_invalid_ids(self, mock_query):
        """Test with invalid issue IDs"""
        result = await get_issue_labels(["abc", "def"], "token")
        assert result == {}
        mock_query.assert_not_called()

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_get_labels_mixed_valid_invalid_ids(self, mock_query):
        """Test with mix of valid and invalid issue IDs"""
        mock_query.return_value = []
        
        result = await get_issue_labels(["123", "abc", "456"], "token")
        
        # Should only query with valid IDs
        mock_query.assert_called_once()
        sql_call = mock_query.call_args[0][0]
        assert "'123','456'" in sql_call
        assert "'abc'" not in sql_call

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_get_labels_exception(self, mock_query):
        """Test exception handling"""
        mock_query.side_effect = Exception("Database error")
        
        result = await get_issue_labels(["123"], "token")
        assert result == {}


class TestGetIssueComments:
    """Test cases for get_issue_comments function"""

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    @patch('database.format_snowflake_row')
    async def test_get_comments_success(self, mock_format, mock_query):
        """Test successful comment retrieval"""
        mock_query.return_value = [
            ["comment1", "123", "public", "Comment body 1", "2024-01-01", "2024-01-02"],
            ["comment2", "123", "private", "Comment body 2", "2024-01-03", "2024-01-04"]
        ]
        
        mock_format.side_effect = [
            {
                "ID": "comment1", "ISSUEID": "123", "ROLELEVEL": "public",
                "BODY": "Comment body 1", "CREATED": "2024-01-01", "UPDATED": "2024-01-02"
            },
            {
                "ID": "comment2", "ISSUEID": "123", "ROLELEVEL": "private",
                "BODY": "Comment body 2", "CREATED": "2024-01-03", "UPDATED": "2024-01-04"
            }
        ]
        
        result = await get_issue_comments(["123"], "token")
        
        expected = {
            "123": [
                {
                    "id": "comment1", "role_level": "public", "body": "Comment body 1",
                    "created": "2024-01-01", "updated": "2024-01-02"
                },
                {
                    "id": "comment2", "role_level": "private", "body": "Comment body 2",
                    "created": "2024-01-03", "updated": "2024-01-04"
                }
            ]
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_comments_empty_input(self):
        """Test with empty issue IDs list"""
        result = await get_issue_comments([], "token")
        assert result == {}

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_get_comments_exception(self, mock_query):
        """Test exception handling"""
        mock_query.side_effect = Exception("Database error")
        
        result = await get_issue_comments(["123"], "token")
        assert result == {}


class TestGetIssueLinks:
    """Test cases for get_issue_links function"""

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    @patch('database.format_snowflake_row')
    async def test_get_links_success(self, mock_format, mock_query):
        """Test successful link retrieval"""
        mock_query.return_value = [
            [
                "link1", "123", "456", "1", "blocks", "is blocked by", "blocks",
                "TEST-1", "TEST-2", "Source summary", "Destination summary"
            ]
        ]
        
        mock_format.return_value = {
            "LINK_ID": "link1", "SOURCE": "123", "DESTINATION": "456", "SEQUENCE": "1",
            "LINKNAME": "blocks", "INWARD": "is blocked by", "OUTWARD": "blocks",
            "SOURCE_KEY": "TEST-1", "DESTINATION_KEY": "TEST-2",
            "SOURCE_SUMMARY": "Source summary", "DESTINATION_SUMMARY": "Destination summary"
        }
        
        result = await get_issue_links(["123"], "token")
        
        assert "123" in result
        assert len(result["123"]) == 1
        link = result["123"][0]
        assert link["relationship"] == "outward"
        assert link["related_issue_id"] == "456"
        assert link["related_issue_key"] == "TEST-2"

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    @patch('database.format_snowflake_row')
    async def test_get_links_bidirectional(self, mock_format, mock_query):
        """Test that links appear for both source and destination issues"""
        mock_query.return_value = [
            [
                "link1", "123", "456", "1", "blocks", "is blocked by", "blocks",
                "TEST-1", "TEST-2", "Source summary", "Destination summary"
            ]
        ]
        
        mock_format.return_value = {
            "LINK_ID": "link1", "SOURCE": "123", "DESTINATION": "456", "SEQUENCE": "1",
            "LINKNAME": "blocks", "INWARD": "is blocked by", "OUTWARD": "blocks",
            "SOURCE_KEY": "TEST-1", "DESTINATION_KEY": "TEST-2",
            "SOURCE_SUMMARY": "Source summary", "DESTINATION_SUMMARY": "Destination summary"
        }
        
        result = await get_issue_links(["123", "456"], "token")
        
        # Should have links for both issues
        assert "123" in result
        assert "456" in result
        
        # Check source perspective
        source_link = result["123"][0]
        assert source_link["relationship"] == "outward"
        assert source_link["related_issue_id"] == "456"
        
        # Check destination perspective
        dest_link = result["456"][0]
        assert dest_link["relationship"] == "inward"
        assert dest_link["related_issue_id"] == "123"

    @pytest.mark.asyncio
    async def test_get_links_empty_input(self):
        """Test with empty issue IDs list"""
        result = await get_issue_links([], "token")
        assert result == {}

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_get_links_exception(self, mock_query):
        """Test exception handling"""
        mock_query.side_effect = Exception("Database error")
        
        result = await get_issue_links(["123"], "token")
        assert result == {}
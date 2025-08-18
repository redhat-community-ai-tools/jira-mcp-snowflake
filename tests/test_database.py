import json
import os
import sys
import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Add src directory to path before importing local modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from database import (  # noqa: E402
    sanitize_sql_value,
    make_snowflake_request,
    execute_snowflake_query,
    format_snowflake_row,
    parse_snowflake_timestamp,
    get_issue_labels,
    get_issue_comments,
    get_issue_links,
    get_issue_enrichment_data_concurrent,
    execute_queries_in_batches,
    format_snowflake_rows_concurrent,
    get_connection_pool,
    get_cache_key,
    get_from_cache,
    set_in_cache,
    clear_cache,
    cleanup_resources,
    load_private_key,
    generate_jwt_token,
    get_auth_token,
    SnowflakeAuthenticationError,
    is_jwt_token_expired,
    get_cached_jwt_token,
    cache_jwt_token,
    clear_jwt_token_cache
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
        with pytest.raises(SnowflakeAuthenticationError) as exc_info:
            await make_snowflake_request("endpoint", "POST", {"test": "data"})
        assert "Authentication token could not be obtained" in str(exc_info.value)

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
    @patch('database.get_from_cache')
    @patch('database.set_in_cache')
    async def test_query_with_pagination(self, mock_set_cache, mock_get_cache, mock_track, mock_request):
        """Test query execution with pagination"""
        # Mock cache miss
        mock_get_cache.return_value = None
        
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
    @patch('database.clear_cache')
    async def test_query_resultset_format(self, mock_clear_cache, mock_track, mock_request):
        """Test query execution with resultSet format"""
        mock_clear_cache()  # Clear cache before test
        mock_request.return_value = {
            "resultSet": {
                "data": [["row1col1", "row1col2"]]
            }
        }

        result = await execute_snowflake_query("SELECT * FROM test", "token", use_cache=False)

        assert len(result) == 1
        assert result[0] == ["row1col1", "row1col2"]

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    @patch('database.clear_cache')
    async def test_query_no_response(self, mock_clear_cache, mock_track, mock_request):
        """Test query execution when no response is returned"""
        mock_clear_cache()  # Clear cache before test
        mock_request.return_value = None

        result = await execute_snowflake_query("SELECT * FROM test", "token", use_cache=False)

        assert result == []
        mock_track.assert_called_once()

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    @patch('database.clear_cache')
    async def test_query_exception(self, mock_clear_cache, mock_track, mock_request):
        """Test query execution when exception occurs"""
        mock_clear_cache()  # Clear cache before test
        mock_request.side_effect = Exception("Database error")

        result = await execute_snowflake_query("SELECT * FROM test", "token", use_cache=False)

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
            ("1753767533.658000000 -43200", "2025-06-29T05:38:53.658000+00:00"),  # -30 days
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

        await get_issue_labels(["123", "abc", "456"], "token")

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
    @patch('database.clear_cache')
    async def test_get_comments_exception(self, mock_clear_cache, mock_query):
        """Test exception handling"""
        mock_clear_cache()  # Clear cache before test
        mock_query.side_effect = Exception("Database error")

        result = await get_issue_comments(["123"], "token", use_cache=False)
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
    @patch('database.clear_cache')
    async def test_get_links_exception(self, mock_clear_cache, mock_query):
        """Test exception handling"""
        mock_clear_cache()  # Clear cache before test
        mock_query.side_effect = Exception("Database error")

        result = await get_issue_links(["123"], "token", use_cache=False)
        assert result == {}


class TestCacheFunctionality:
    """Test cases for caching functionality"""

    def test_get_cache_key(self):
        """Test cache key generation"""
        key = get_cache_key("test_op", param1="value1", param2="value2")
        assert key == "test_op:param1:value1:param2:value2"

    def test_get_cache_key_with_none_values(self):
        """Test cache key generation with None values"""
        key = get_cache_key("test_op", param1="value1", param2=None)
        assert key == "test_op:param1:value1"

    def test_get_cache_key_empty_params(self):
        """Test cache key generation with no parameters"""
        key = get_cache_key("test_op")
        assert key == "test_op"

    @patch('database.ENABLE_CACHING', True)
    def test_cache_operations_enabled(self):
        """Test cache operations when caching is enabled"""
        clear_cache()  # Start fresh
        
        # Test cache miss
        result = get_from_cache("test_key")
        assert result is None
        
        # Test cache set and hit
        test_data = {"test": "data"}
        set_in_cache("test_key", test_data)
        result = get_from_cache("test_key")
        assert result == test_data

    @patch('database.ENABLE_CACHING', False)
    def test_cache_operations_disabled(self):
        """Test cache operations when caching is disabled"""
        # Should not cache when disabled
        set_in_cache("test_key", {"test": "data"})
        result = get_from_cache("test_key")
        assert result is None

    def test_clear_cache(self):
        """Test cache clearing"""
        set_in_cache("test_key", {"test": "data"})
        clear_cache()
        result = get_from_cache("test_key")
        assert result is None


class TestConnectionPool:
    """Test cases for connection pool functionality"""

    def test_get_connection_pool(self):
        """Test connection pool creation"""
        pool = get_connection_pool()
        assert pool is not None
        assert hasattr(pool, 'max_connections')
        assert hasattr(pool, 'timeout')

    def test_connection_pool_singleton(self):
        """Test that connection pool is a singleton"""
        pool1 = get_connection_pool()
        pool2 = get_connection_pool()
        assert pool1 is pool2

    @pytest.mark.asyncio
    async def test_connection_pool_client(self):
        """Test connection pool client creation"""
        pool = get_connection_pool()
        client = await pool.get_client()
        assert client is not None
        assert hasattr(client, 'request')

    @pytest.mark.asyncio
    async def test_cleanup_resources(self):
        """Test resource cleanup"""
        # Should not raise any exceptions
        await cleanup_resources()


class TestMakeSnowflakeRequestWithCaching:
    """Test cases for make_snowflake_request with caching"""

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.get_connection_pool')
    @patch('database._throttler')
    async def test_request_with_caching_enabled(self, mock_throttler, mock_pool):
        """Test request with caching enabled"""
        # Setup mocks
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = MagicMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        
        mock_pool_instance = MagicMock()
        mock_pool_instance.get_client = AsyncMock(return_value=mock_client)
        mock_pool.return_value = mock_pool_instance
        
        mock_throttler.__aenter__ = AsyncMock()
        mock_throttler.__aexit__ = AsyncMock()

        clear_cache()  # Start fresh
        
        # First request should hit the API
        result1 = await make_snowflake_request("test", "GET", {"param": "value"}, use_cache=True)
        assert result1 == {"data": "test"}
        
        # Second identical request should hit cache (if caching is enabled)
        result2 = await make_snowflake_request("test", "GET", {"param": "value"}, use_cache=True)
        assert result2 == {"data": "test"}

    @pytest.mark.asyncio
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.get_connection_pool')
    @patch('database._throttler')
    async def test_request_with_caching_disabled(self, mock_throttler, mock_pool):
        """Test request with caching disabled"""
        # Setup mocks
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = MagicMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        
        mock_pool_instance = MagicMock()
        mock_pool_instance.get_client = AsyncMock(return_value=mock_client)
        mock_pool.return_value = mock_pool_instance
        
        mock_throttler.__aenter__ = AsyncMock()
        mock_throttler.__aexit__ = AsyncMock()

        result = await make_snowflake_request("test", "POST", {"param": "value"}, use_cache=False)
        assert result == {"data": "test"}
        mock_client.request.assert_called_once()


class TestExecuteSnowflakeQueryWithCaching:
    """Test cases for execute_snowflake_query with caching"""

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_with_cache_hit(self, mock_track, mock_request):
        """Test query execution with cache hit"""
        mock_request.return_value = {"data": [["row1", "row2"]]}
        
        clear_cache()  # Start fresh
        
        # First query should hit the API
        result1 = await execute_snowflake_query("SELECT * FROM test", "token", use_cache=True)
        assert result1 == [["row1", "row2"]]
        
        # Second identical query should hit cache (if caching is enabled)
        result2 = await execute_snowflake_query("SELECT * FROM test", "token", use_cache=True)
        assert result2 == [["row1", "row2"]]

    @pytest.mark.asyncio
    @patch('database.make_snowflake_request')
    @patch('database.track_snowflake_query')
    async def test_query_without_caching(self, mock_track, mock_request):
        """Test query execution without caching"""
        mock_request.return_value = {"data": [["row1", "row2"]]}
        
        result = await execute_snowflake_query("INSERT INTO test VALUES (1)", "token", use_cache=False)
        assert result == [["row1", "row2"]]
        mock_request.assert_called_once()


class TestConcurrentFunctions:
    """Test cases for concurrent processing functions"""

    @pytest.mark.asyncio
    @patch('database.get_issue_labels')
    @patch('database.get_issue_comments') 
    @patch('database.get_issue_links')
    async def test_get_issue_enrichment_data_concurrent_success(self, mock_links, mock_comments, mock_labels):
        """Test successful concurrent data enrichment"""
        # Setup mocks
        mock_labels.return_value = {"123": ["bug", "urgent"]}
        mock_comments.return_value = {"123": [{"id": "c1", "body": "comment"}]}
        mock_links.return_value = {"123": [{"id": "l1", "type": "blocks"}]}
        
        labels, comments, links = await get_issue_enrichment_data_concurrent(["123"], "token")
        
        assert labels == {"123": ["bug", "urgent"]}
        assert comments == {"123": [{"id": "c1", "body": "comment"}]}
        assert links == {"123": [{"id": "l1", "type": "blocks"}]}
        
        # Verify all functions were called concurrently
        mock_labels.assert_called_once_with(["123"], "token", True)
        mock_comments.assert_called_once_with(["123"], "token", True)
        mock_links.assert_called_once_with(["123"], "token", True)

    @pytest.mark.asyncio
    async def test_get_issue_enrichment_data_concurrent_empty_input(self):
        """Test concurrent data enrichment with empty input"""
        labels, comments, links = await get_issue_enrichment_data_concurrent([], "token")
        
        assert labels == {}
        assert comments == {}
        assert links == {}

    @pytest.mark.asyncio
    @patch('database.get_issue_labels')
    @patch('database.get_issue_comments') 
    @patch('database.get_issue_links')
    async def test_get_issue_enrichment_data_concurrent_with_exception(self, mock_links, mock_comments, mock_labels):
        """Test concurrent data enrichment with one function failing"""
        # Setup mocks - one fails, others succeed
        mock_labels.side_effect = Exception("Labels error")
        mock_comments.return_value = {"123": [{"id": "c1", "body": "comment"}]}
        mock_links.return_value = {"123": [{"id": "l1", "type": "blocks"}]}
        
        labels, comments, links = await get_issue_enrichment_data_concurrent(["123"], "token")
        
        # Failed operation should return empty dict
        assert labels == {}
        assert comments == {"123": [{"id": "c1", "body": "comment"}]}
        assert links == {"123": [{"id": "l1", "type": "blocks"}]}

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_execute_queries_in_batches_success(self, mock_query):
        """Test successful batch query execution"""
        # Mock query returns
        mock_query.side_effect = [
            [["row1", "col1"]],
            [["row2", "col2"]],
            [["row3", "col3"]]
        ]
        
        queries = ["SELECT 1", "SELECT 2", "SELECT 3"]
        results = await execute_queries_in_batches(queries, "token", batch_size=2)
        
        assert len(results) == 3
        assert results[0] == [["row1", "col1"]]
        assert results[1] == [["row2", "col2"]]
        assert results[2] == [["row3", "col3"]]
        
        # Should be called 3 times (once per query)
        assert mock_query.call_count == 3

    @pytest.mark.asyncio
    async def test_execute_queries_in_batches_empty_input(self):
        """Test batch query execution with empty input"""
        results = await execute_queries_in_batches([], "token")
        assert results == []

    @pytest.mark.asyncio
    @patch('database.execute_snowflake_query')
    async def test_execute_queries_in_batches_with_exception(self, mock_query):
        """Test batch query execution with one query failing"""
        # First query succeeds, second fails
        mock_query.side_effect = [
            [["row1", "col1"]],
            Exception("Query error")
        ]
        
        queries = ["SELECT 1", "SELECT 2"]
        results = await execute_queries_in_batches(queries, "token", batch_size=1)
        
        assert len(results) == 2
        assert results[0] == [["row1", "col1"]]
        assert results[1] == []  # Failed query returns empty list

    @pytest.mark.asyncio
    @patch('database._thread_pool')
    async def test_format_snowflake_rows_concurrent_small_dataset(self, mock_thread_pool):
        """Test concurrent row formatting with small dataset"""
        import asyncio
        
        rows = [["val1", "val2"], ["val3", "val4"]]
        columns = ["col1", "col2"]
        
        # Mock the thread pool execution
        expected_result = [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]
        
        loop = asyncio.get_event_loop()
        future = asyncio.Future()
        future.set_result(expected_result)
        loop.run_in_executor = AsyncMock(return_value=expected_result)
        
        with patch('database.asyncio.get_event_loop', return_value=loop):
            result = await format_snowflake_rows_concurrent(rows, columns, batch_size=100)
        
        assert result == expected_result

    @pytest.mark.asyncio
    async def test_format_snowflake_rows_concurrent_empty_input(self):
        """Test concurrent row formatting with empty input"""
        result = await format_snowflake_rows_concurrent([], ["col1", "col2"])
        assert result == []


class TestPrivateKeyAuthentication:
    """Test cases for private key authentication functions"""

    @patch('database.JWT_AVAILABLE', True)
    @patch('builtins.open', MagicMock())
    @patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
    def test_load_private_key_success(self, mock_load_key):
        """Test successful private key loading"""
        # Mock private key
        mock_private_key = MagicMock()
        mock_pem_bytes = b"-----BEGIN PRIVATE KEY-----\nMOCK_KEY\n-----END PRIVATE KEY-----"
        mock_private_key.private_bytes.return_value = mock_pem_bytes
        mock_load_key.return_value = mock_private_key

        # Mock file reading
        mock_file_data = b"mock_key_data"
        with patch('builtins.open', lambda path, mode: MagicMock(__enter__=lambda self: MagicMock(read=lambda: mock_file_data), __exit__=lambda *args: None)):
            result = load_private_key("/path/to/key.pem", "passphrase")

        assert result == mock_pem_bytes
        mock_load_key.assert_called_once_with(mock_file_data, password=b"passphrase")

    @patch('database.JWT_AVAILABLE', True)
    @patch('builtins.open', MagicMock())
    @patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
    def test_load_private_key_without_passphrase(self, mock_load_key):
        """Test private key loading without passphrase"""
        mock_private_key = MagicMock()
        mock_pem_bytes = b"-----BEGIN PRIVATE KEY-----\nMOCK_KEY\n-----END PRIVATE KEY-----"
        mock_private_key.private_bytes.return_value = mock_pem_bytes
        mock_load_key.return_value = mock_private_key

        mock_file_data = b"mock_key_data"
        with patch('builtins.open', lambda path, mode: MagicMock(__enter__=lambda self: MagicMock(read=lambda: mock_file_data), __exit__=lambda *args: None)):
            result = load_private_key("/path/to/key.pem")

        assert result == mock_pem_bytes
        mock_load_key.assert_called_once_with(mock_file_data, password=None)

    @patch('database.JWT_AVAILABLE', False)
    def test_load_private_key_jwt_not_available(self):
        """Test private key loading when JWT is not available"""
        with pytest.raises(ImportError) as exc_info:
            load_private_key("/path/to/key.pem")
        
        assert "JWT and cryptography libraries are required" in str(exc_info.value)

    @patch('database.JWT_AVAILABLE', True)
    def test_load_private_key_file_not_found(self):
        """Test private key loading when file doesn't exist"""
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                load_private_key("/nonexistent/key.pem")

    @patch('database.JWT_AVAILABLE', True)
    @patch('builtins.open', MagicMock())
    @patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
    def test_load_private_key_invalid_key(self, mock_load_key):
        """Test private key loading with invalid key"""
        mock_load_key.side_effect = Exception("Invalid key format")

        mock_file_data = b"invalid_key_data"
        with patch('builtins.open', lambda path, mode: MagicMock(__enter__=lambda self: MagicMock(read=lambda: mock_file_data), __exit__=lambda *args: None)):
            with pytest.raises(ValueError) as exc_info:
                load_private_key("/path/to/invalid_key.pem")
        
        assert "Unable to load private key" in str(exc_info.value)

    @patch('database.JWT_AVAILABLE', True)
    @patch('database.load_private_key')
    @patch('jwt.encode')
    @patch('database.uuid.uuid4')
    def test_generate_jwt_token_success(self, mock_uuid, mock_jwt_encode, mock_load_key):
        """Test successful JWT token generation"""
        # Mock dependencies
        mock_private_key = b"mock_private_key"
        mock_load_key.return_value = mock_private_key
        mock_jwt_encode.return_value = "mock_jwt_token"
        mock_uuid.return_value = MagicMock(__str__=lambda self: "mock-uuid")

        result = generate_jwt_token("test_user", "/path/to/key.pem", "passphrase")

        assert result == "mock_jwt_token"
        mock_load_key.assert_called_once_with("/path/to/key.pem", "passphrase")
        mock_jwt_encode.assert_called_once()
        
        # Check JWT payload structure
        call_args = mock_jwt_encode.call_args
        payload = call_args[0][0]
        assert payload['iss'] == "test_user.SNOWFLAKE"
        assert payload['sub'] == "test_user.SNOWFLAKE"
        assert 'iat' in payload
        assert 'exp' in payload
        assert 'jti' in payload

    @patch('database.JWT_AVAILABLE', False)
    def test_generate_jwt_token_jwt_not_available(self):
        """Test JWT token generation when JWT is not available"""
        with pytest.raises(ImportError) as exc_info:
            generate_jwt_token("test_user", "/path/to/key.pem")
        
        assert "JWT and cryptography libraries are required" in str(exc_info.value)

    @patch('database.JWT_AVAILABLE', True)
    @patch('database.load_private_key')
    def test_generate_jwt_token_key_load_error(self, mock_load_key):
        """Test JWT token generation when key loading fails"""
        mock_load_key.side_effect = Exception("Key loading failed")

        with pytest.raises(ValueError) as exc_info:
            generate_jwt_token("test_user", "/path/to/key.pem")
        
        assert "Unable to generate JWT token" in str(exc_info.value)

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'token')
    @patch('database.SNOWFLAKE_TOKEN', 'test_token')
    def test_get_auth_token_with_explicit_token(self):
        """Test get_auth_token with explicitly provided token"""
        result = get_auth_token("explicit_token")
        assert result == "explicit_token"

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'token')
    @patch('database.SNOWFLAKE_TOKEN', 'configured_token')
    def test_get_auth_token_token_method(self):
        """Test get_auth_token with token authentication method"""
        result = get_auth_token()
        assert result == "configured_token"

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', 'test_user')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PATH', '/path/to/key.pem')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', 'passphrase')
    @patch('database.generate_jwt_token')
    def test_get_auth_token_private_key_method(self, mock_generate_jwt):
        """Test get_auth_token with private key authentication method"""
        mock_generate_jwt.return_value = "jwt_token"

        result = get_auth_token()

        assert result == "jwt_token"
        mock_generate_jwt.assert_called_once_with('test_user', '/path/to/key.pem', 'passphrase')

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', None)
    def test_get_auth_token_private_key_missing_username(self):
        """Test get_auth_token with private key method but missing username"""
        with pytest.raises(ValueError) as exc_info:
            get_auth_token()
        
        assert "SNOWFLAKE_USERNAME is required" in str(exc_info.value)

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', 'test_user')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PATH', None)
    def test_get_auth_token_private_key_missing_path(self):
        """Test get_auth_token with private key method but missing key path"""
        with pytest.raises(ValueError) as exc_info:
            get_auth_token()
        
        assert "SNOWFLAKE_PRIVATE_KEY_PATH is required" in str(exc_info.value)

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'invalid_method')
    def test_get_auth_token_invalid_method(self):
        """Test get_auth_token with invalid authentication method"""
        with pytest.raises(ValueError) as exc_info:
            get_auth_token()
        
        assert "Invalid SNOWFLAKE_AUTH_METHOD: invalid_method" in str(exc_info.value)

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', 'test_user')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PATH', '/path/to/key.pem')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None)
    @patch('database.generate_jwt_token')
    def test_get_auth_token_private_key_no_passphrase(self, mock_generate_jwt):
        """Test get_auth_token with private key method and no passphrase"""
        mock_generate_jwt.return_value = "jwt_token"

        result = get_auth_token()

        assert result == "jwt_token"
        mock_generate_jwt.assert_called_once_with('test_user', '/path/to/key.pem', None)


class TestMakeSnowflakeRequestWithPrivateKey:
    """Test cases for make_snowflake_request with private key authentication"""

    @pytest.mark.asyncio
    @patch('database.get_auth_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_make_request_with_private_key_auth(self, mock_client_class, mock_get_auth_token):
        """Test make_snowflake_request with private key authentication"""
        # Mock auth token generation
        mock_get_auth_token.return_value = "jwt_token"

        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance

        result = await make_snowflake_request("endpoint", "POST", {"test": "data"})

        assert result == {"data": []}
        
        # Verify the JWT token was used in Authorization header
        args, kwargs = mock_client_instance.request.call_args
        headers = kwargs['headers']
        assert headers['Authorization'] == 'Bearer jwt_token'
        
        mock_get_auth_token.assert_called_once_with(None)

    @pytest.mark.asyncio
    @patch('database.get_auth_token')
    async def test_make_request_auth_token_generation_failure(self, mock_get_auth_token):
        """Test make_snowflake_request when auth token generation fails"""
        mock_get_auth_token.side_effect = Exception("JWT generation failed")

        with pytest.raises(SnowflakeAuthenticationError) as exc_info:
            await make_snowflake_request("endpoint", "POST", {"test": "data"})
        
        assert "Authentication failed: JWT generation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('database.get_auth_token')
    async def test_make_request_auth_token_none(self, mock_get_auth_token):
        """Test make_snowflake_request when auth token is None"""
        mock_get_auth_token.return_value = None

        with pytest.raises(SnowflakeAuthenticationError) as exc_info:
            await make_snowflake_request("endpoint", "POST", {"test": "data"})
        
        assert "Authentication token could not be obtained" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('database.get_auth_token')
    @patch('database.SNOWFLAKE_BASE_URL', 'https://test.snowflake.com')
    @patch('database.httpx.AsyncClient')
    async def test_make_request_with_provided_token_override(self, mock_client_class, mock_get_auth_token):
        """Test that provided token overrides auth method"""
        # Mock auth token generation (should not be called for the actual auth)
        mock_get_auth_token.return_value = "provided_token"

        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()

        mock_client_instance = AsyncMock()
        mock_client_instance.request = AsyncMock(return_value=mock_response)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance

        result = await make_snowflake_request("endpoint", "POST", {"test": "data"}, "provided_token")

        assert result == {"data": []}
        
        # Verify the provided token was used
        args, kwargs = mock_client_instance.request.call_args
        headers = kwargs['headers']
        assert headers['Authorization'] == 'Bearer provided_token'
        
        # Verify get_auth_token was called with the provided token
        mock_get_auth_token.assert_called_once_with("provided_token")


class TestJWTTokenCaching:
    """Test cases for JWT token caching functionality"""

    def setup_method(self):
        """Clear token cache before each test"""
        clear_jwt_token_cache()

    def teardown_method(self):
        """Clear token cache after each test"""
        clear_jwt_token_cache()

    @patch('database.JWT_AVAILABLE', True)
    @patch('jwt.decode')
    def test_is_jwt_token_expired_valid_token(self, mock_jwt_decode):
        """Test JWT token expiration check for valid token"""
        from datetime import datetime, timezone, timedelta
        
        # Mock token that expires in 30 minutes
        future_exp = int((datetime.now(timezone.utc) + timedelta(minutes=30)).timestamp())
        mock_jwt_decode.return_value = {'exp': future_exp}
        
        result = is_jwt_token_expired("valid_token")
        assert result is False
        mock_jwt_decode.assert_called_once_with("valid_token", options={"verify_signature": False})

    @patch('database.JWT_AVAILABLE', True)
    @patch('jwt.decode')
    def test_is_jwt_token_expired_expired_token(self, mock_jwt_decode):
        """Test JWT token expiration check for expired token"""
        from datetime import datetime, timezone, timedelta
        
        # Mock token that expired 10 minutes ago
        past_exp = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp())
        mock_jwt_decode.return_value = {'exp': past_exp}
        
        result = is_jwt_token_expired("expired_token")
        assert result is True

    @patch('database.JWT_AVAILABLE', True)
    @patch('jwt.decode')
    def test_is_jwt_token_expired_soon_to_expire(self, mock_jwt_decode):
        """Test JWT token expiration check for token expiring soon"""
        from datetime import datetime, timezone, timedelta
        
        # Mock token that expires in 2 minutes (less than 5-minute buffer)
        soon_exp = int((datetime.now(timezone.utc) + timedelta(minutes=2)).timestamp())
        mock_jwt_decode.return_value = {'exp': soon_exp}
        
        result = is_jwt_token_expired("soon_to_expire_token")
        assert result is True  # Should be considered expired due to buffer

    @patch('database.JWT_AVAILABLE', True)
    @patch('jwt.decode')
    def test_is_jwt_token_expired_no_exp_claim(self, mock_jwt_decode):
        """Test JWT token expiration check for token without exp claim"""
        mock_jwt_decode.return_value = {'sub': 'user', 'iss': 'issuer'}  # No exp claim
        
        result = is_jwt_token_expired("token_without_exp")
        assert result is True

    @patch('database.JWT_AVAILABLE', True)
    @patch('jwt.decode')
    def test_is_jwt_token_expired_decode_error(self, mock_jwt_decode):
        """Test JWT token expiration check when decoding fails"""
        mock_jwt_decode.side_effect = Exception("Invalid token")
        
        result = is_jwt_token_expired("invalid_token")
        assert result is True

    @patch('database.JWT_AVAILABLE', False)
    def test_is_jwt_token_expired_jwt_not_available(self):
        """Test JWT token expiration check when JWT is not available"""
        result = is_jwt_token_expired("any_token")
        assert result is True

    @patch('database.is_jwt_token_expired')
    def test_cache_and_get_jwt_token(self, mock_is_expired):
        """Test caching and retrieving JWT tokens"""
        cache_key = "test_user:/path/to/key"
        test_token = "test.jwt.token"
        
        # Mock token as not expired
        mock_is_expired.return_value = False
        
        # Initially, token should not be cached
        cached_token = get_cached_jwt_token(cache_key)
        assert cached_token is None
        
        # Cache the token
        cache_jwt_token(cache_key, test_token)
        
        # Now it should be retrievable
        cached_token = get_cached_jwt_token(cache_key)
        assert cached_token == test_token

    @patch('database.is_jwt_token_expired')
    def test_get_cached_jwt_token_expired(self, mock_is_expired):
        """Test that expired tokens are removed from cache"""
        cache_key = "test_user:/path/to/key"
        test_token = "expired.jwt.token"
        
        # Cache the token
        cache_jwt_token(cache_key, test_token)
        
        # Mock the token as expired
        mock_is_expired.return_value = True
        
        # Getting the token should return None and remove it from cache
        cached_token = get_cached_jwt_token(cache_key)
        assert cached_token is None
        
        # Verify it's actually removed from cache
        mock_is_expired.return_value = False  # Reset mock
        cached_token = get_cached_jwt_token(cache_key)
        assert cached_token is None  # Should still be None

    @patch('database.is_jwt_token_expired')
    def test_clear_jwt_token_cache(self, mock_is_expired):
        """Test clearing the entire JWT token cache"""
        # Mock tokens as not expired
        mock_is_expired.return_value = False
        
        # Cache multiple tokens
        cache_jwt_token("user1:/key1", "token1")
        cache_jwt_token("user2:/key2", "token2")
        
        # Verify tokens are cached
        assert get_cached_jwt_token("user1:/key1") == "token1"
        assert get_cached_jwt_token("user2:/key2") == "token2"
        
        # Clear cache
        clear_jwt_token_cache()
        
        # Verify all tokens are gone
        assert get_cached_jwt_token("user1:/key1") is None
        assert get_cached_jwt_token("user2:/key2") is None

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', 'test_user')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PATH', '/path/to/key.pem')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None)
    @patch('database.generate_jwt_token')
    @patch('database.is_jwt_token_expired')
    def test_get_auth_token_caching_behavior(self, mock_is_expired, mock_generate_jwt):
        """Test that get_auth_token properly caches JWT tokens"""
        mock_generate_jwt.return_value = "new_jwt_token"
        mock_is_expired.return_value = False  # Token is not expired
        
        # First call should generate a new token
        token1 = get_auth_token()
        assert token1 == "new_jwt_token"
        assert mock_generate_jwt.call_count == 1
        
        # Second call should use cached token (no new generation)
        token2 = get_auth_token()
        assert token2 == "new_jwt_token"
        assert mock_generate_jwt.call_count == 1  # Should not increase

    @patch('database.SNOWFLAKE_AUTH_METHOD', 'private_key')
    @patch('database.SNOWFLAKE_USERNAME', 'test_user')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PATH', '/path/to/key.pem')
    @patch('database.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None)
    @patch('database.generate_jwt_token')
    @patch('database.is_jwt_token_expired')
    def test_get_auth_token_expired_token_regeneration(self, mock_is_expired, mock_generate_jwt):
        """Test that expired tokens trigger regeneration"""
        mock_generate_jwt.side_effect = ["first_token", "second_token"]
        
        # First call generates and caches token
        mock_is_expired.return_value = False
        token1 = get_auth_token()
        assert token1 == "first_token"
        assert mock_generate_jwt.call_count == 1
        
        # Second call with expired token should regenerate
        mock_is_expired.return_value = True
        token2 = get_auth_token()
        assert token2 == "second_token"
        assert mock_generate_jwt.call_count == 2

    @pytest.mark.asyncio
    @patch('database.is_jwt_token_expired')
    async def test_cleanup_resources_clears_jwt_cache(self, mock_is_expired):
        """Test that cleanup_resources clears JWT token cache"""
        mock_is_expired.return_value = False  # Mock tokens as not expired
        
        # Cache some tokens
        cache_jwt_token("user1:/key1", "token1")
        cache_jwt_token("user2:/key2", "token2")
        
        # Verify tokens are cached
        assert get_cached_jwt_token("user1:/key1") == "token1"
        
        # Call cleanup_resources (we need to call the real function)
        from database import cleanup_resources as real_cleanup
        await real_cleanup()
        
        # Verify JWT cache is cleared
        assert get_cached_jwt_token("user1:/key1") is None
        assert get_cached_jwt_token("user2:/key2") is None

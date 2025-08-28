import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Dict, Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from tools import get_snowflake_token, register_tools


class TestGetSnowflakeToken:
    """Test cases for get_snowflake_token function"""
    
    @patch('tools.MCP_TRANSPORT', 'stdio')
    @patch('tools.SNOWFLAKE_TOKEN', 'test_token')
    def test_get_token_stdio_transport(self):
        """Test token retrieval for stdio transport"""
        mcp = MagicMock()
        token = get_snowflake_token(mcp)
        assert token == 'test_token'

    @patch('tools.MCP_TRANSPORT', 'stdio')
    @patch('tools.INTERNAL_GATEWAY', 'true')
    @patch('tools.SNOWFLAKE_TOKEN', 'test_token')
    def test_get_token_internal_gateway(self):
        """Test token retrieval when internal gateway is enabled"""
        mcp = MagicMock()
        token = get_snowflake_token(mcp)
        assert token == 'test_token'

    @patch('tools.MCP_TRANSPORT', 'http')
    @patch('tools.INTERNAL_GATEWAY', 'false')
    def test_get_token_from_headers_success(self):
        """Test successful token retrieval from request headers"""
        mcp = MagicMock()
        context = MagicMock()
        context.request_context.request.headers = {"X-Snowflake-Token": "header_token"}
        mcp.get_context.return_value = context
        
        token = get_snowflake_token(mcp)
        assert token == "header_token"

    @patch('tools.MCP_TRANSPORT', 'http')
    @patch('tools.INTERNAL_GATEWAY', 'false')
    def test_get_token_from_headers_empty(self):
        """Test token retrieval when header is empty"""
        mcp = MagicMock()
        context = MagicMock()
        context.request_context.request.headers = {"X-Snowflake-Token": ""}
        mcp.get_context.return_value = context
        
        token = get_snowflake_token(mcp)
        assert token is None

    @patch('tools.MCP_TRANSPORT', 'http')
    @patch('tools.INTERNAL_GATEWAY', 'false')
    def test_get_token_missing_header(self):
        """Test token retrieval when header is missing"""
        mcp = MagicMock()
        context = MagicMock()
        context.request_context.request.headers = {}
        mcp.get_context.return_value = context
        
        token = get_snowflake_token(mcp)
        assert token is None

    @patch('tools.MCP_TRANSPORT', 'http')
    @patch('tools.INTERNAL_GATEWAY', 'false')
    def test_get_token_no_context(self):
        """Test token retrieval when no context is available"""
        mcp = MagicMock()
        mcp.get_context.return_value = None
        
        token = get_snowflake_token(mcp)
        assert token is None

    @patch('tools.MCP_TRANSPORT', 'http')
    @patch('tools.INTERNAL_GATEWAY', 'false')
    def test_get_token_exception(self):
        """Test token retrieval when an exception occurs"""
        mcp = MagicMock()
        mcp.get_context.side_effect = Exception("Test error")
        
        token = get_snowflake_token(mcp)
        assert token is None


class TestRegisterTools:
    """Test cases for register_tools function and individual tool implementations"""

    @pytest.fixture
    def mock_mcp(self):
        """Create a mock MCP instance"""
        mcp = MagicMock()
        # Store registered functions for testing
        mcp._registered_tools = []
        
        def mock_tool_decorator():
            def decorator(func):
                mcp._registered_tools.append(func)
                return func
            return decorator
        
        mcp.tool = mock_tool_decorator
        return mcp

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all external dependencies"""
        with patch('tools.get_snowflake_token') as mock_token, \
             patch('tools.execute_snowflake_query') as mock_query, \
             patch('tools.get_issue_enrichment_data_concurrent') as mock_enrichment, \
             patch('tools.format_snowflake_row') as mock_format, \
             patch('tools.sanitize_sql_value') as mock_sanitize:
            
            mock_token.return_value = 'test_token'
            mock_query.return_value = []
            mock_enrichment.return_value = ({}, {}, {}, {})  # labels, comments, links, status_changes
            mock_format.return_value = {}
            mock_sanitize.side_effect = lambda x: str(x).replace("'", "''")
            
            yield {
                'token': mock_token,
                'query': mock_query,
                'enrichment': mock_enrichment,
                'format': mock_format,
                'sanitize': mock_sanitize
            }

    def test_register_tools(self, mock_mcp):
        """Test that register_tools completes without error"""
        register_tools(mock_mcp)
        # Verify that 4 tools were registered (removed list_jira_components)
        assert len(mock_mcp._registered_tools) == 4

    @pytest.mark.asyncio
    async def test_list_jira_issues_no_token(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues when no token is available"""
        mock_dependencies['token'].return_value = None
        
        register_tools(mock_mcp)
        
        # Get the registered function
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues()
        assert result['error'] == "Snowflake token not available"
        assert result['issues'] == []

    @pytest.mark.asyncio
    async def test_list_jira_issues_success(self, mock_mcp, mock_dependencies):
        """Test successful list_jira_issues execution"""
        # Mock successful query result
        mock_dependencies['query'].return_value = [
            ['123', 'TEST-1', 'TEST', '1', 'Bug', 'Test Summary', 'Short desc', 'Full description',
             'High', 'Open', None, '2024-01-01', '2024-01-02', None, None, '0', '1', None, None, None,
             'Test Component', 'Test Component Desc', 'N', 'N']
        ]
        
        mock_dependencies['format'].return_value = {
            'ID': '123', 'ISSUE_KEY': 'TEST-1', 'PROJECT': 'TEST', 'ISSUENUM': '1',
            'ISSUETYPE': 'Bug', 'SUMMARY': 'Test Summary', 'DESCRIPTION_TRUNCATED': 'Short desc',
            'DESCRIPTION': 'Full description', 'PRIORITY': 'High', 'ISSUESTATUS': 'Open',
            'RESOLUTION': None, 'CREATED': '2024-01-01', 'UPDATED': '2024-01-02',
            'DUEDATE': None, 'RESOLUTIONDATE': None, 'VOTES': '0', 'WATCHES': '1',
            'ENVIRONMENT': None, 'COMPONENT': None, 'FIXFOR': None,
            'COMPONENT_NAME': 'Test Component', 'COMPONENT_DESCRIPTION': 'Test Component Desc',
            'COMPONENT_ARCHIVED': 'N', 'COMPONENT_DELETED': 'N'
        }
        
        mock_dependencies['enrichment'].return_value = (
            {'123': ['label1', 'label2']},  # labels
            {'123': [{'id': 'c1', 'body': 'comment'}]},  # comments  
            {'123': [{'link_id': '456'}]},  # links
            {'TEST-1': [{'from_status': 'New', 'to_status': 'Open'}]}  # status_changes
        )
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues(project='TEST', limit=10)
        
        assert 'issues' in result
        assert 'total_returned' in result
        assert 'filters_applied' in result
        assert result['filters_applied']['project'] == 'TEST'
        assert result['filters_applied']['limit'] == 10

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with various filters"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues(
            project='TEST',
            issue_type='Bug',
            status='Open',
            priority='High',
            search_text='test search',
            timeframe=14
        )
        
        # Verify SQL conditions were built correctly
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.PROJECT = 'TEST'" in sql_call
        assert "i.ISSUETYPE = 'Bug'" in sql_call
        assert "i.ISSUESTATUS = 'Open'" in sql_call
        assert "i.PRIORITY = 'High'" in sql_call
        assert "LOWER(i.SUMMARY) LIKE '%test search%'" in sql_call
        
        # Verify timeframe condition is included (filters by ANY date: created, updated, or resolved)
        timeframe_condition = "(i.CREATED >= DATEADD(DAY, -14, CURRENT_TIMESTAMP()) OR i.UPDATED >= DATEADD(DAY, -14, CURRENT_TIMESTAMP()) OR i.RESOLUTIONDATE >= DATEADD(DAY, -14, CURRENT_TIMESTAMP()))"
        assert timeframe_condition in sql_call
        
        # Verify filters_applied includes timeframe
        assert result['filters_applied']['timeframe'] == 14

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_not_found(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details when issue is not found"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-999'])
        assert result['found_issues'] == {}
        assert result['not_found'] == ['TEST-999']
        assert result['total_found'] == 0
        assert result['total_requested'] == 1

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_success(self, mock_mcp, mock_dependencies):
        """Test successful get_jira_issue_details execution"""
        mock_dependencies['query'].return_value = [
            ['123', 'TEST-1', 'TEST', '1', 'Bug', 'Test Summary', 'Full description',
             'High', 'Open', None, '2024-01-01', '2024-01-02', None, None, '0', '1',
             None, None, None, '3600', '1800', '900', 'WF-1', None, 'N', None, None, None, None, None]
        ]
        
        mock_dependencies['format'].return_value = {
            'ID': '123', 'ISSUE_KEY': 'TEST-1', 'PROJECT': 'TEST', 'ISSUENUM': '1',
            'ISSUETYPE': 'Bug', 'SUMMARY': 'Test Summary', 'DESCRIPTION': 'Full description',
            'PRIORITY': 'High', 'ISSUESTATUS': 'Open', 'RESOLUTION': None,
            'CREATED': '2024-01-01', 'UPDATED': '2024-01-02', 'DUEDATE': None,
            'RESOLUTIONDATE': None, 'VOTES': '0', 'WATCHES': '1', 'ENVIRONMENT': None,
            'COMPONENT': None, 'FIXFOR': None, 'TIMEORIGINALESTIMATE': '3600',
            'TIMEESTIMATE': '1800', 'TIMESPENT': '900', 'WORKFLOW_ID': 'WF-1',
            'SECURITY': None, 'ARCHIVED': 'N', 'ARCHIVEDDATE': None, 'COMPONENT_NAME': None
        }
        
        mock_dependencies['enrichment'].return_value = (
            {'123': ['label1']},  # labels
            {'123': [{'id': '789', 'body': 'Comment'}]},  # comments
            {'123': [{'link_id': '456'}]},  # links
            {'TEST-1': [{'from_status': 'New', 'to_status': 'Open'}]}  # status_changes
        )
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-1'])
        
        assert result['found_issues']['TEST-1']['key'] == 'TEST-1'
        assert result['found_issues']['TEST-1']['summary'] == 'Test Summary'
        assert 'labels' in result['found_issues']['TEST-1']
        assert 'comments' in result['found_issues']['TEST-1']
        assert 'links' in result['found_issues']['TEST-1']
        assert result['not_found'] == []
        assert result['total_found'] == 1
        assert result['total_requested'] == 1

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_multiple_issues_success(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details with multiple valid issue keys"""
        mock_dependencies['query'].return_value = [
            ['123', 'TEST-1', 'TEST', '1', 'Bug', 'Test Summary 1', 'Full description 1',
             'High', 'Open', None, '2024-01-01', '2024-01-02', None, None, '0', '1',
             None, None, None, '3600', '1800', '900', 'WF-1', None, 'N', None, None, None, None, None],
            ['124', 'TEST-2', 'TEST', '2', 'Feature', 'Test Summary 2', 'Full description 2',
             'Medium', 'In Progress', None, '2024-01-03', '2024-01-04', None, None, '1', '2',
             None, None, None, '7200', '3600', '1800', 'WF-2', None, 'N', None, None, None, None, None]
        ]
        
        def mock_format_side_effect(row, columns):
            row_dict = dict(zip(columns, row))
            return row_dict
        
        mock_dependencies['format'].side_effect = mock_format_side_effect
        
        mock_dependencies['enrichment'].return_value = (
            {'123': ['label1'], '124': ['label2', 'label3']},  # labels
            {'123': [{'id': '789', 'body': 'Comment 1'}], '124': [{'id': '790', 'body': 'Comment 2'}]},  # comments
            {'123': [{'link_id': '456'}], '124': [{'link_id': '457'}]},  # links
            {'TEST-1': [{'from_status': 'New', 'to_status': 'Open'}], 'TEST-2': [{'from_status': 'Open', 'to_status': 'Closed'}]}  # status_changes
        )
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-1', 'TEST-2'])
        
        assert len(result['found_issues']) == 2
        assert 'TEST-1' in result['found_issues']
        assert 'TEST-2' in result['found_issues']
        assert result['found_issues']['TEST-1']['summary'] == 'Test Summary 1'
        assert result['found_issues']['TEST-2']['summary'] == 'Test Summary 2'
        assert result['not_found'] == []
        assert result['total_found'] == 2
        assert result['total_requested'] == 2

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_mixed_results(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details with some found and some not found issue keys"""
        mock_dependencies['query'].return_value = [
            ['123', 'TEST-1', 'TEST', '1', 'Bug', 'Test Summary', 'Full description',
             'High', 'Open', None, '2024-01-01', '2024-01-02', None, None, '0', '1',
             None, None, None, '3600', '1800', '900', 'WF-1', None, 'N', None, None, None, None, None]
        ]
        
        def mock_format_side_effect(row, columns):
            return dict(zip(columns, row))
        
        mock_dependencies['format'].side_effect = mock_format_side_effect
        
        mock_dependencies['enrichment'].return_value = (
            {'123': ['label1']},  # labels
            {'123': [{'id': '789', 'body': 'Comment'}]},  # comments
            {'123': [{'link_id': '456'}]},  # links
            {'TEST-1': [{'from_status': 'New', 'to_status': 'Open'}]}  # status_changes
        )
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-1', 'TEST-999', 'TEST-998'])
        
        assert len(result['found_issues']) == 1
        assert 'TEST-1' in result['found_issues']
        assert result['found_issues']['TEST-1']['summary'] == 'Test Summary'
        assert set(result['not_found']) == {'TEST-999', 'TEST-998'}
        assert result['total_found'] == 1
        assert result['total_requested'] == 3

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_all_not_found(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details with multiple non-existent issue keys"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-999', 'TEST-998', 'TEST-997'])
        
        assert result['found_issues'] == {}
        assert set(result['not_found']) == {'TEST-999', 'TEST-998', 'TEST-997'}
        assert result['total_found'] == 0
        assert result['total_requested'] == 3

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_empty_list(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details with an empty list input"""
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details([])
        
        assert result['found_issues'] == {}
        assert result['not_found'] == []
        assert result['total_found'] == 0
        assert result['total_requested'] == 0

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_duplicate_keys(self, mock_mcp, mock_dependencies):
        """Test get_jira_issue_details with duplicate issue keys in the list"""
        mock_dependencies['query'].return_value = [
            ['123', 'TEST-1', 'TEST', '1', 'Bug', 'Test Summary', 'Full description',
             'High', 'Open', None, '2024-01-01', '2024-01-02', None, None, '0', '1',
             None, None, None, '3600', '1800', '900', 'WF-1', None, 'N', None, None, None, None, None]
        ]
        
        def mock_format_side_effect(row, columns):
            return dict(zip(columns, row))
        
        mock_dependencies['format'].side_effect = mock_format_side_effect
        
        mock_dependencies['enrichment'].return_value = (
            {'123': ['label1']},  # labels
            {'123': [{'id': '789', 'body': 'Comment'}]},  # comments
            {'123': [{'link_id': '456'}]},  # links
            {'TEST-1': [{'from_status': 'New', 'to_status': 'Open'}]}  # status_changes
        )
        
        register_tools(mock_mcp)
        get_jira_issue_details = mock_mcp._registered_tools[1]
        
        result = await get_jira_issue_details(['TEST-1', 'TEST-1', 'TEST-1'])
        
        # Should only have one instance in found_issues despite duplicates
        assert len(result['found_issues']) == 1
        assert 'TEST-1' in result['found_issues']
        assert result['found_issues']['TEST-1']['summary'] == 'Test Summary'
        assert result['not_found'] == []
        assert result['total_found'] == 1
        assert result['total_requested'] == 3  # Still counts all requested

    @pytest.mark.asyncio
    async def test_get_jira_project_summary_success(self, mock_mcp, mock_dependencies):
        """Test successful get_jira_project_summary execution"""
        mock_dependencies['query'].return_value = [
            ['TEST', 'Open', 'High', '5'],
            ['TEST', 'Open', 'Medium', '10'],
            ['PROD', 'Closed', 'Low', '3']
        ]
        
        def mock_format_side_effect(row, columns):
            return dict(zip(columns, row))
        
        mock_dependencies['format'].side_effect = mock_format_side_effect
        
        register_tools(mock_mcp)
        get_jira_project_summary = mock_mcp._registered_tools[2]
        
        result = await get_jira_project_summary()
        
        assert result['total_issues'] == 18
        assert result['total_projects'] == 2
        assert 'TEST' in result['projects']
        assert 'PROD' in result['projects']
        assert result['projects']['TEST']['total_issues'] == 15

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_component_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with component filters"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues(
            project='TEST',
            components='frontend'
        )
        
        # Verify SQL conditions were built correctly for component filters
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "LOWER(c.CNAME) LIKE '%frontend%'" in sql_call
        assert "JOIN JIRA_DB.RHAI_MARTS.JIRA_COMPONENT_RHAI c" in sql_call
        
        # Verify filters_applied includes component filters
        assert result['filters_applied']['components'] == 'frontend'

    @pytest.mark.asyncio
    async def test_list_jira_issues_without_component_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues without component filters still includes component joins"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues(project='TEST')
        
        # Verify SQL ALWAYS includes component joins now
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "LEFT JOIN JIRA_DB.RHAI_MARTS.JIRA_COMPONENT_RHAI c" in sql_call
        assert "LEFT JOIN JIRA_DB.RHAI_MARTS.JIRA_NODEASSOCIATION_RHAI na" in sql_call
        assert "i.PROJECT = 'TEST'" in sql_call  # Should always have table alias now

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_multiple_component_filters_sql(self, mock_mcp, mock_dependencies):
        """Builds OR conditions for multiple component filters (generic names)"""
        mock_dependencies['query'].return_value = []

        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]

        components = 'frontend, backend'
        await list_jira_issues(project='PROJECT', issue_type='1', status='Open', components=components)

        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "LOWER(c.CNAME) LIKE '%frontend%'" in sql_call
        assert "LOWER(c.DESCRIPTION) LIKE '%frontend%'" in sql_call
        assert "LOWER(c.CNAME) LIKE '%backend%'" in sql_call
        assert "LOWER(c.DESCRIPTION) LIKE '%backend%'" in sql_call
        assert " OR " in sql_call

    @pytest.mark.asyncio
    async def test_list_jira_issues_component_aggregation_dedup(self, mock_mcp, mock_dependencies):
        """De-duplicates issues and aggregates components into a unique list (generic names)"""
        # Two rows for same issue id (simulating duplicates from joins)
        mock_dependencies['query'].return_value = [
            ['123', 'PROJ-9282', 'frontend||backend'],
            ['123', 'PROJ-9282', 'frontend||backend'],
        ]

        # Map minimal fields regardless of the internal columns list
        def mock_format_side_effect(row, columns):
            # Only provide fields used by aggregation logic
            return {
                'ID': row[0],
                'ISSUE_KEY': row[1],
                'COMPONENT_NAMES': row[2],
            }

        mock_dependencies['format'].side_effect = mock_format_side_effect

        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]

        result = await list_jira_issues(project='PROJ', issue_type='1', status='Open', components='frontend, backend')

        assert result['total_returned'] == 1
        assert len(result['issues']) == 1
        issue = result['issues'][0]
        assert issue['component'] == ['frontend', 'backend']
        assert issue['component_name'] == 'frontend'

    @pytest.mark.asyncio
    async def test_list_jira_issues_skips_rows_with_missing_id(self, mock_mcp, mock_dependencies):
        """Ensure rows with missing ID are safely skipped (branch coverage for continue)."""
        # One row returned, but formatted row has ID=None to trigger skip
        mock_dependencies['query'].return_value = [["ignored"]]

        def mock_format_side_effect(row, columns):
            return {"ID": None}

        mock_dependencies['format'].side_effect = mock_format_side_effect

        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]

        result = await list_jira_issues(project='TEST')

        # No issues should be returned; path with `continue` is exercised
        assert result['total_returned'] == 0
        assert result['issues'] == []

    @pytest.mark.asyncio
    @patch('tools.get_issue_links')
    async def test_get_jira_issue_links_success(self, mock_get_links, mock_mcp, mock_dependencies):
        """Test successful get_jira_issue_links execution"""
        # First query returns issue ID
        mock_dependencies['query'].return_value = [['123']]
        # Mock get_issue_links function directly since this tool uses it directly
        mock_get_links.return_value = {'123': [{'link_id': '456', 'type': 'blocks'}]}
        
        register_tools(mock_mcp)
        get_jira_issue_links = mock_mcp._registered_tools[3]
        
        result = await get_jira_issue_links('TEST-1')
        
        assert result['issue_key'] == 'TEST-1'
        assert result['issue_id'] == '123'
        assert 'links' in result
        assert result['total_links'] == 1

    @pytest.mark.asyncio
    async def test_exception_handling(self, mock_mcp, mock_dependencies):
        """Test exception handling in tools"""
        mock_dependencies['token'].side_effect = Exception("Database error")
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        result = await list_jira_issues()
        assert 'error' in result
        assert 'Database error' in result['error']

    @pytest.mark.asyncio
    async def test_list_jira_issues_default_timeframe(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with default timeframe (0 - disabled)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Call without specifying timeframe (should use default 0 - disabled)
        result = await list_jira_issues(project='TEST')
        
        # Verify SQL conditions do NOT include any timeframe filter since default is 0
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "DATEADD(DAY, -" not in sql_call  # No date filters should be present
        
        # Verify filters_applied includes default timeframe
        assert result['filters_applied']['timeframe'] == 0

    @pytest.mark.asyncio
    async def test_list_jira_issues_custom_timeframe(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with custom timeframe (7 days)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with custom timeframe
        result = await list_jira_issues(project='PROJECT', timeframe=7)
        
        # Verify SQL conditions include custom timeframe (filters by ANY date: created, updated, or resolved)
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        timeframe_condition = "(i.CREATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) OR i.UPDATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) OR i.RESOLUTIONDATE >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()))"
        assert timeframe_condition in sql_call
        
        # Verify filters_applied includes custom timeframe
        assert result['filters_applied']['timeframe'] == 7

    @pytest.mark.asyncio
    async def test_list_jira_issues_zero_timeframe(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with timeframe=0 (disabled)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with timeframe=0 (should disable timeframe filtering)
        result = await list_jira_issues(project='TEST', timeframe=0)
        
        # Verify SQL conditions do NOT include timeframe
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "DATEADD" not in sql_call
        assert "CURRENT_TIMESTAMP()" not in sql_call
        
        # Verify filters_applied includes timeframe=0
        assert result['filters_applied']['timeframe'] == 0

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_issue_keys(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with issue_keys parameter"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with single issue key
        result = await list_jira_issues(issue_keys=['TEST-123'])
        
        # Verify SQL conditions include issue key filter
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.ISSUE_KEY IN ('TEST-123')" in sql_call
        
        # Verify filters_applied includes issue_keys
        assert result['filters_applied']['issue_keys'] == ['TEST-123']

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_multiple_issue_keys(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with multiple issue keys"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with multiple issue keys
        issue_keys = ['TEST-123', 'PROJ-456', 'BUG-789']
        result = await list_jira_issues(issue_keys=issue_keys)
        
        # Verify SQL conditions include all issue keys
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.ISSUE_KEY IN ('TEST-123', 'PROJ-456', 'BUG-789')" in sql_call
        
        # Verify filters_applied includes all issue_keys
        assert result['filters_applied']['issue_keys'] == issue_keys

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_issue_keys_and_other_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with issue_keys combined with other filters"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with issue_keys and other filters
        result = await list_jira_issues(
            issue_keys=['TEST-123', 'TEST-456'],
            project='TEST',
            status='Open',
            priority='High'
        )
        
        # Verify SQL conditions include all filters
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.ISSUE_KEY IN ('TEST-123', 'TEST-456')" in sql_call
        assert "i.PROJECT = 'TEST'" in sql_call
        assert "i.ISSUESTATUS = 'Open'" in sql_call
        assert "i.PRIORITY = 'High'" in sql_call
        
        # Verify filters_applied includes all parameters
        assert result['filters_applied']['issue_keys'] == ['TEST-123', 'TEST-456']
        assert result['filters_applied']['project'] == 'TEST'
        assert result['filters_applied']['status'] == 'Open'
        assert result['filters_applied']['priority'] == 'High'

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_empty_issue_keys(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with empty issue_keys list"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with empty issue_keys list (should be ignored)
        result = await list_jira_issues(issue_keys=[], project='TEST')
        
        # Verify SQL conditions do NOT include issue key filter
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.ISSUE_KEY IN" not in sql_call
        assert "i.PROJECT = 'TEST'" in sql_call
        
        # Verify filters_applied includes empty issue_keys
        assert result['filters_applied']['issue_keys'] == []
        assert result['filters_applied']['project'] == 'TEST'

    @pytest.mark.asyncio
    async def test_list_jira_issues_issue_keys_sql_sanitization(self, mock_mcp, mock_dependencies):
        """Test that issue_keys are properly sanitized for SQL injection protection"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with issue keys that contain SQL-sensitive characters
        issue_keys = ["TEST-123", "PROJ'456", "BUG\"789"]
        result = await list_jira_issues(issue_keys=issue_keys)
        
        # Verify sanitize_sql_value was called for each issue key
        mock_dependencies['sanitize'].assert_any_call("TEST-123")
        mock_dependencies['sanitize'].assert_any_call("PROJ'456")
        mock_dependencies['sanitize'].assert_any_call("BUG\"789")
        
        # Verify SQL contains sanitized values
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.ISSUE_KEY IN" in sql_call

    @pytest.mark.asyncio
    async def test_list_jira_issues_large_timeframe(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with large timeframe (365 days)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with large timeframe
        result = await list_jira_issues(timeframe=365)
        
        # Verify SQL conditions include large timeframe (filters by ANY date: created, updated, or resolved)
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        timeframe_condition = "(i.CREATED >= DATEADD(DAY, -365, CURRENT_TIMESTAMP()) OR i.UPDATED >= DATEADD(DAY, -365, CURRENT_TIMESTAMP()) OR i.RESOLUTIONDATE >= DATEADD(DAY, -365, CURRENT_TIMESTAMP()))"
        assert timeframe_condition in sql_call
        
        # Verify filters_applied includes large timeframe
        assert result['filters_applied']['timeframe'] == 365

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_created_days_filter(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with created_days filter (overrides timeframe)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with both timeframe and created_days (created_days should take precedence)
        result = await list_jira_issues(project='TEST', timeframe=30, created_days=7)
        
        # Verify SQL conditions use created_days, not timeframe
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.CREATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())" in sql_call
        assert "i.CREATED >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())" not in sql_call
        
        # Verify filters_applied includes both values
        assert result['filters_applied']['timeframe'] == 30
        assert result['filters_applied']['created_days'] == 7

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_updated_days_filter(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with updated_days filter"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with updated_days filter
        result = await list_jira_issues(project='TEST', updated_days=14)
        
        # Verify SQL conditions include updated filter
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.UPDATED >= DATEADD(DAY, -14, CURRENT_TIMESTAMP())" in sql_call
        
        # Verify filters_applied includes updated_days
        assert result['filters_applied']['updated_days'] == 14

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_resolved_days_filter(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with resolved_days filter"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with resolved_days filter
        result = await list_jira_issues(project='TEST', resolved_days=21)
        
        # Verify SQL conditions include resolution filter
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.RESOLUTIONDATE >= DATEADD(DAY, -21, CURRENT_TIMESTAMP())" in sql_call
        
        # Verify filters_applied includes resolved_days
        assert result['filters_applied']['resolved_days'] == 21

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_multiple_date_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with multiple specific date filters"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with all three specific date filters
        result = await list_jira_issues(
            project='TEST',
            created_days=7,
            updated_days=14,
            resolved_days=21
        )
        
        # Verify SQL conditions include all three filters (AND logic)
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "i.CREATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())" in sql_call
        assert "i.UPDATED >= DATEADD(DAY, -14, CURRENT_TIMESTAMP())" in sql_call
        assert "i.RESOLUTIONDATE >= DATEADD(DAY, -21, CURRENT_TIMESTAMP())" in sql_call
        
        # Verify filters_applied includes all values
        assert result['filters_applied']['created_days'] == 7
        assert result['filters_applied']['updated_days'] == 14
        assert result['filters_applied']['resolved_days'] == 21

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_zero_date_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with zero values for date filters (should be ignored)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with zero values (should be ignored)
        result = await list_jira_issues(
            project='TEST',
            timeframe=0,
            created_days=0,
            updated_days=0,
            resolved_days=0
        )
        
        # Verify no date filters are applied
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        assert "DATEADD" not in sql_call
        assert "CURRENT_TIMESTAMP()" not in sql_call
        
        # Verify filters_applied includes all zero values
        assert result['filters_applied']['timeframe'] == 0
        assert result['filters_applied']['created_days'] == 0
        assert result['filters_applied']['updated_days'] == 0
        assert result['filters_applied']['resolved_days'] == 0

    @pytest.mark.asyncio
    async def test_list_jira_issues_with_zero_specific_date_filters(self, mock_mcp, mock_dependencies):
        """Test list_jira_issues with zero values for specific date filters (defaults)"""
        mock_dependencies['query'].return_value = []
        
        register_tools(mock_mcp)
        list_jira_issues = mock_mcp._registered_tools[0]
        
        # Test with default zero values and a timeframe (should fall back to timeframe)
        result = await list_jira_issues(
            project='TEST',
            timeframe=7,
            created_days=0,
            updated_days=0,
            resolved_days=0
        )
        
        # Verify only timeframe filter is applied (filters by ANY date: created, updated, or resolved)
        mock_dependencies['query'].assert_called_once()
        sql_call = mock_dependencies['query'].call_args[0][0]
        timeframe_condition = "(i.CREATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) OR i.UPDATED >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) OR i.RESOLUTIONDATE >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()))"
        assert timeframe_condition in sql_call
        
        # Verify filters_applied includes zero values
        assert result['filters_applied']['timeframe'] == 7
        assert result['filters_applied']['created_days'] == 0
        assert result['filters_applied']['updated_days'] == 0
        assert result['filters_applied']['resolved_days'] == 0


class TestConcurrentProcessingIntegration:
    """Test cases for concurrent processing integration in tools"""

    @pytest.fixture
    def mock_mcp_with_concurrent(self):
        """Mock MCP server with concurrent processing mocks"""
        mcp = MagicMock()
        mcp._registered_tools = []

        def mock_tool(func=None):
            def decorator(f):
                mcp._registered_tools.append(f)
                return f
            return decorator(func) if func else decorator

        mcp.tool = mock_tool
        return mcp

    @pytest.fixture
    def mock_concurrent_dependencies(self):
        """Mock dependencies for concurrent processing tests"""
        with patch('tools.get_snowflake_token') as mock_token, \
             patch('tools.execute_snowflake_query') as mock_query, \
             patch('tools.get_issue_enrichment_data_concurrent') as mock_concurrent, \
             patch('tools.track_concurrent_operation') as mock_track, \
             patch('tools.format_snowflake_row') as mock_format:
            
            mock_token.return_value = 'test_token'
            # Set default format return value
            mock_format.return_value = {
                'ID': '123', 'ISSUE_KEY': 'TEST-1', 'PROJECT': 'PROJECT', 'ISSUENUM': '1',
                'ISSUETYPE': 'Bug', 'SUMMARY': 'Test issue', 'DESCRIPTION_TRUNCATED': 'Short desc',
                'DESCRIPTION': 'Full description', 'PRIORITY': 'High', 'ISSUESTATUS': 'Open',
                'RESOLUTION': None, 'CREATED': '2024-01-01', 'UPDATED': '2024-01-02',
                'DUEDATE': None, 'RESOLUTIONDATE': None, 'VOTES': 0, 'WATCHES': 0,
                'ENVIRONMENT': 'test', 'COMPONENT': 'comp', 'FIXFOR': 'v1.0',
                'COMPONENT_NAME': 'Test Component', 'COMPONENT_DESCRIPTION': 'Test Component Desc',
                'COMPONENT_ARCHIVED': 'N', 'COMPONENT_DELETED': 'N'
            }
            yield {
                'token': mock_token,
                'query': mock_query,
                'concurrent': mock_concurrent,
                'track': mock_track,
                'format': mock_format
            }

    @pytest.mark.asyncio
    async def test_list_jira_issues_uses_concurrent_processing(self, mock_mcp_with_concurrent, mock_concurrent_dependencies):
        """Test that list_jira_issues uses concurrent processing for enrichment"""
        # Setup mocks
        mock_concurrent_dependencies['query'].return_value = [
            ["123", "TEST-1", "PROJECT", "1", "Bug", "Test issue", "Short desc", "Full description",
             "High", "Open", None, "2024-01-01", "2024-01-02", None, None, 0, 0, "test", "comp", "v1.0",
             "Test Component", "Test Component Desc", "N", "N"]
        ]
        
        mock_concurrent_dependencies['concurrent'].return_value = (
            {"123": ["bug", "urgent"]},  # labels
            {"123": [{"id": "c1", "body": "comment"}]},  # comments
            {"123": [{"id": "l1", "type": "blocks"}]},  # links
            {"TEST-1": [{"from_status": "New", "to_status": "Open"}]}  # status_changes
        )
        
        register_tools(mock_mcp_with_concurrent)
        list_jira_issues = mock_mcp_with_concurrent._registered_tools[0]
        
        # Execute the function
        result = await list_jira_issues(project="TEST")
        
        # Verify concurrent processing was used
        mock_concurrent_dependencies['concurrent'].assert_called_once()
        mock_concurrent_dependencies['track'].assert_called_with("issue_enrichment")
        
        # Verify enrichment data was added to issues
        assert len(result['issues']) == 1
        issue = result['issues'][0]
        assert issue['labels'] == ["bug", "urgent"]
        assert issue['links'] == [{"id": "l1", "type": "blocks"}]
        # Comments should not be included in list view
        assert 'comments' not in issue or issue.get('comments') == []

    @pytest.mark.asyncio
    async def test_get_jira_issue_details_uses_concurrent_processing(self, mock_mcp_with_concurrent, mock_concurrent_dependencies):
        """Test that get_jira_issue_details uses concurrent processing"""
        # Setup mocks
        mock_concurrent_dependencies['query'].return_value = [
            ["123", "TEST-1", "PROJECT", "1", "Bug", "Test issue", "Full description",
             "High", "Open", None, "2024-01-01", "2024-01-02", None, None, 0, 0, "test", "comp", "v1.0",
             "8h", "4h", "2h", "workflow1", None, False, None, None, None, None, None]
        ]
        
        mock_concurrent_dependencies['concurrent'].return_value = (
            {"123": ["bug", "urgent"]},  # labels
            {"123": [{"id": "c1", "body": "Test comment", "created": "2024-01-01"}]},  # comments
            {"123": [{"id": "l1", "type": "blocks"}]},  # links
            {"TEST-1": [{"from_status": "New", "to_status": "Open"}]}  # status_changes
        )
        
        register_tools(mock_mcp_with_concurrent)
        get_jira_issue_details = mock_mcp_with_concurrent._registered_tools[1]
        
        # Execute the function
        result = await get_jira_issue_details(["TEST-1"])
        
        # Verify concurrent processing was used
        mock_concurrent_dependencies['concurrent'].assert_called_once()
        mock_concurrent_dependencies['track'].assert_called_with("multiple_issue_enrichment")
        
        # Verify all enrichment data was added to the found issue
        assert result['found_issues']['TEST-1']['labels'] == ["bug", "urgent"]
        assert result['found_issues']['TEST-1']['comments'] == [{"id": "c1", "body": "Test comment", "created": "2024-01-01"}]
        assert result['found_issues']['TEST-1']['links'] == [{"id": "l1", "type": "blocks"}]
        assert result['not_found'] == []
        assert result['total_found'] == 1
        assert result['total_requested'] == 1

    @pytest.mark.asyncio
    async def test_concurrent_processing_handles_exceptions(self, mock_mcp_with_concurrent, mock_concurrent_dependencies):
        """Test that concurrent processing handles exceptions gracefully"""
        # Setup mocks - concurrent processing fails
        mock_concurrent_dependencies['query'].return_value = [
            ["123", "TEST-1", "PROJECT", "1", "Bug", "Test issue", "Short desc", "Full description",
             "High", "Open", None, "2024-01-01", "2024-01-02", None, None, 0, 0, "test", "comp", "v1.0",
             "Test Component", "Test Component Desc", "N", "N"]
        ]
        
        # Mock concurrent processing to return empty data (simulating exception handling)
        mock_concurrent_dependencies['concurrent'].return_value = ({}, {}, {}, {})
        
        register_tools(mock_mcp_with_concurrent)
        list_jira_issues = mock_mcp_with_concurrent._registered_tools[0]
        
        # Execute the function - should not raise exception
        result = await list_jira_issues(project="TEST")
        
        # Should still return the basic issue data
        assert len(result['issues']) == 1
        issue = result['issues'][0]
        assert issue['key'] == "TEST-1"
        # Enrichment data should be empty due to exception
        assert issue.get('labels', []) == []
        assert issue.get('links', []) == []

    @pytest.mark.asyncio
    async def test_concurrent_processing_with_empty_results(self, mock_mcp_with_concurrent, mock_concurrent_dependencies):
        """Test concurrent processing with empty enrichment results"""
        # Setup mocks
        mock_concurrent_dependencies['query'].return_value = [
            ["123", "TEST-1", "PROJECT", "1", "Bug", "Test issue", "Short desc", "Full description",
             "High", "Open", None, "2024-01-01", "2024-01-02", None, None, 0, 0, "test", "comp", "v1.0",
             "Test Component", "Test Component Desc", "N", "N"]
        ]
        
        # Mock concurrent processing returns empty results
        mock_concurrent_dependencies['concurrent'].return_value = ({}, {}, {}, {})
        
        register_tools(mock_mcp_with_concurrent)
        list_jira_issues = mock_mcp_with_concurrent._registered_tools[0]
        
        # Execute the function
        result = await list_jira_issues(project="TEST")
        
        # Verify enrichment data is empty but structure is preserved
        assert len(result['issues']) == 1
        issue = result['issues'][0]
        assert issue['labels'] == []
        assert issue['links'] == []

    @pytest.mark.asyncio
    async def test_concurrent_operation_tracking(self, mock_mcp_with_concurrent, mock_concurrent_dependencies):
        """Test that concurrent operations are properly tracked"""
        # Setup mocks
        mock_concurrent_dependencies['query'].return_value = []
        mock_concurrent_dependencies['concurrent'].return_value = ({}, {}, {}, {})
        
        register_tools(mock_mcp_with_concurrent)
        list_jira_issues = mock_mcp_with_concurrent._registered_tools[0]
        get_jira_issue_details = mock_mcp_with_concurrent._registered_tools[1]
        
        # Execute both functions
        await list_jira_issues(project="TEST")
        
        # Mock issue details query
        mock_concurrent_dependencies['query'].return_value = [
            ["123", "TEST-1", "PROJECT", "1", "Bug", "Test issue", "Full description",
             "High", "Open", None, "2024-01-01", "2024-01-02", None, None, 0, 0, "test", "comp", "v1.0",
             "8h", "4h", "2h", "workflow1", None, False, None, None, None, None, None]
        ]
        
        await get_jira_issue_details(["TEST-1"])
        
        # Verify tracking was called for both operation types
        expected_calls = [
            call("issue_enrichment"),
            call("multiple_issue_enrichment")
        ]
        mock_concurrent_dependencies['track'].assert_has_calls(expected_calls)
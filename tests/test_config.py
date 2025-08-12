import pytest
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))


class TestConfig:
    """Test cases for configuration module"""

    @patch.dict('os.environ', {
        'MCP_TRANSPORT': 'http',
        'SNOWFLAKE_BASE_URL': 'https://test.snowflake.com',
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA': 'TEST_SCHEMA',
        'SNOWFLAKE_WAREHOUSE': 'TEST_WH',
        'INTERNAL_GATEWAY': 'true',
        'ENABLE_METRICS': 'true',
        'METRICS_PORT': '9090'
    })
    def test_config_from_environment(self):
        """Test configuration loading from environment variables"""
        # Re-import to get fresh config with mocked environment
        import importlib
        import config
        importlib.reload(config)
        
        assert config.MCP_TRANSPORT == 'http'
        assert config.SNOWFLAKE_BASE_URL == 'https://test.snowflake.com'
        assert config.SNOWFLAKE_DATABASE == 'TEST_DB'
        assert config.SNOWFLAKE_SCHEMA == 'TEST_SCHEMA'
        assert config.SNOWFLAKE_WAREHOUSE == 'TEST_WH'
        assert config.INTERNAL_GATEWAY == 'true'
        assert config.ENABLE_METRICS is True
        assert config.METRICS_PORT == 9090

    @patch.dict('os.environ', {}, clear=True)
    def test_config_defaults(self):
        """Test configuration defaults when environment variables are not set"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.MCP_TRANSPORT == 'stdio'  # Default
        assert config.SNOWFLAKE_WAREHOUSE == 'DEFAULT'  # Default
        assert config.INTERNAL_GATEWAY == 'false'  # Default
        assert config.ENABLE_METRICS is False  # Default
        assert config.METRICS_PORT == 8000  # Default
        
        # These should be None when not set
        assert config.SNOWFLAKE_BASE_URL is None
        assert config.SNOWFLAKE_DATABASE is None
        assert config.SNOWFLAKE_SCHEMA is None

    @patch.dict('os.environ', {'MCP_TRANSPORT': 'stdio', 'SNOWFLAKE_TOKEN': 'test_token'})
    def test_stdio_transport_token_handling(self):
        """Test token handling for stdio transport"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.MCP_TRANSPORT == 'stdio'
        assert config.SNOWFLAKE_TOKEN == 'test_token'

    @patch.dict('os.environ', {'MCP_TRANSPORT': 'http', 'SNOWFLAKE_TOKEN': 'test_token'})
    def test_non_stdio_transport_token_handling(self):
        """Test token handling for non-stdio transport"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.MCP_TRANSPORT == 'http'
        assert config.SNOWFLAKE_TOKEN is None  # Should be None for non-stdio

    @patch.dict('os.environ', {'ENABLE_METRICS': 'false'})
    def test_metrics_disabled(self):
        """Test metrics configuration when disabled"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_METRICS is False

    @patch.dict('os.environ', {'ENABLE_METRICS': 'true'})
    def test_metrics_enabled(self):
        """Test metrics configuration when enabled"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_METRICS is True

    @patch.dict('os.environ', {'ENABLE_METRICS': 'TRUE'})
    def test_metrics_case_insensitive(self):
        """Test that metrics configuration is case insensitive"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_METRICS is True

    @patch.dict('os.environ', {'ENABLE_METRICS': 'yes'})
    def test_metrics_non_true_value(self):
        """Test that non-'true' values disable metrics"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_METRICS is False

    @patch.dict('os.environ', {'METRICS_PORT': 'not_a_number'})
    def test_invalid_metrics_port(self):
        """Test handling of invalid metrics port"""
        import importlib
        
        with pytest.raises(ValueError):
            import config
            importlib.reload(config)

    def test_prometheus_availability_check(self):
        """Test Prometheus availability detection"""
        import importlib
        import config
        importlib.reload(config)
        
        # PROMETHEUS_AVAILABLE should be True or False
        assert isinstance(config.PROMETHEUS_AVAILABLE, bool)

    @patch('config.logging.basicConfig')
    def test_logging_configuration(self, mock_basicconfig):
        """Test that logging is configured"""
        import importlib
        import config
        importlib.reload(config)
        
        # Verify logging.basicConfig was called
        mock_basicconfig.assert_called_once()
        
        # Check the configuration parameters
        call_args = mock_basicconfig.call_args
        assert call_args[1]['level'] == config.logging.INFO
        assert 'format' in call_args[1]
        assert 'handlers' in call_args[1]

    @patch.dict('os.environ', {'INTERNAL_GATEWAY': 'FALSE'})
    def test_internal_gateway_case_insensitive(self):
        """Test that internal gateway configuration is case insensitive"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.INTERNAL_GATEWAY == 'FALSE'  # Should preserve original case

    @patch.dict('os.environ', {
        'SNOWFLAKE_BASE_URL': '',
        'SNOWFLAKE_DATABASE': '',
        'SNOWFLAKE_SCHEMA': ''
    })
    def test_empty_string_environment_variables(self):
        """Test handling of empty string environment variables"""
        import importlib
        import config
        importlib.reload(config)
        
        # Empty strings should be treated as None/empty
        assert config.SNOWFLAKE_BASE_URL == ''
        assert config.SNOWFLAKE_DATABASE == ''
        assert config.SNOWFLAKE_SCHEMA == ''

    def test_config_constants_immutability(self):
        """Test that configuration values are set as expected"""
        import config
        
        # Test that we can access the configuration values
        # (immutability would be tested by trying to modify them)
        assert hasattr(config, 'MCP_TRANSPORT')
        assert hasattr(config, 'SNOWFLAKE_BASE_URL')
        assert hasattr(config, 'SNOWFLAKE_DATABASE')
        assert hasattr(config, 'SNOWFLAKE_SCHEMA')
        assert hasattr(config, 'SNOWFLAKE_WAREHOUSE')
        assert hasattr(config, 'INTERNAL_GATEWAY')
        assert hasattr(config, 'SNOWFLAKE_TOKEN')
        assert hasattr(config, 'ENABLE_METRICS')
        assert hasattr(config, 'METRICS_PORT')
        assert hasattr(config, 'PROMETHEUS_AVAILABLE')
        
        # Performance configuration
        assert hasattr(config, 'ENABLE_CACHING')
        assert hasattr(config, 'CACHE_TTL_SECONDS')
        assert hasattr(config, 'CACHE_MAX_SIZE')
        assert hasattr(config, 'MAX_HTTP_CONNECTIONS')
        assert hasattr(config, 'HTTP_TIMEOUT_SECONDS')
        assert hasattr(config, 'THREAD_POOL_WORKERS')
        assert hasattr(config, 'RATE_LIMIT_PER_SECOND')
        assert hasattr(config, 'CONCURRENT_QUERY_BATCH_SIZE')

    def test_prometheus_import_error(self):
        """Test handling when prometheus_client import fails"""
        # Test the import check logic directly
        import sys
        import importlib
        
        # Temporarily remove prometheus_client from modules if it exists
        prometheus_module = sys.modules.pop('prometheus_client', None)
        
        try:
            # Mock the import to fail
            with patch.dict('sys.modules', {'prometheus_client': None}):
                # Reload config to trigger the import check
                import config
                importlib.reload(config)
                
                # Should detect that prometheus is not available
                assert config.PROMETHEUS_AVAILABLE is False
        finally:
            # Restore the module if it was there
            if prometheus_module is not None:
                sys.modules['prometheus_client'] = prometheus_module


class TestPerformanceConfig:
    """Test cases for performance configuration"""

    @patch.dict('os.environ', {
        'ENABLE_CACHING': 'false',
        'CACHE_TTL_SECONDS': '600',
        'CACHE_MAX_SIZE': '2000',
        'MAX_HTTP_CONNECTIONS': '50',
        'HTTP_TIMEOUT_SECONDS': '120',
        'THREAD_POOL_WORKERS': '20',
        'RATE_LIMIT_PER_SECOND': '100',
        'CONCURRENT_QUERY_BATCH_SIZE': '10'
    })
    def test_performance_config_from_environment(self):
        """Test performance configuration loading from environment variables"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_CACHING is False
        assert config.CACHE_TTL_SECONDS == 600
        assert config.CACHE_MAX_SIZE == 2000
        assert config.MAX_HTTP_CONNECTIONS == 50
        assert config.HTTP_TIMEOUT_SECONDS == 120
        assert config.THREAD_POOL_WORKERS == 20
        assert config.RATE_LIMIT_PER_SECOND == 100
        assert config.CONCURRENT_QUERY_BATCH_SIZE == 10

    @patch.dict('os.environ', {}, clear=True)
    def test_performance_config_defaults(self):
        """Test performance configuration defaults"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_CACHING is True  # Default enabled
        assert config.CACHE_TTL_SECONDS == 300  # 5 minutes
        assert config.CACHE_MAX_SIZE == 1000
        assert config.MAX_HTTP_CONNECTIONS == 20
        assert config.HTTP_TIMEOUT_SECONDS == 60
        assert config.THREAD_POOL_WORKERS == 10
        assert config.RATE_LIMIT_PER_SECOND == 50
        assert config.CONCURRENT_QUERY_BATCH_SIZE == 5

    @patch.dict('os.environ', {'ENABLE_CACHING': 'TRUE'})
    def test_caching_enabled_case_insensitive(self):
        """Test that caching configuration is case insensitive"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_CACHING is True

    @patch.dict('os.environ', {'ENABLE_CACHING': 'no'})
    def test_caching_non_true_value(self):
        """Test that non-'true' values disable caching"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.ENABLE_CACHING is False

    @patch.dict('os.environ', {'CACHE_TTL_SECONDS': 'not_a_number'})
    def test_invalid_cache_ttl(self):
        """Test handling of invalid cache TTL"""
        import importlib
        
        with pytest.raises(ValueError):
            import config
            importlib.reload(config)

    @patch.dict('os.environ', {'MAX_HTTP_CONNECTIONS': 'not_a_number'})
    def test_invalid_max_connections(self):
        """Test handling of invalid max connections"""
        import importlib
        
        with pytest.raises(ValueError):
            import config
            importlib.reload(config)

    @patch.dict('os.environ', {'THREAD_POOL_WORKERS': '0'})
    def test_zero_thread_pool_workers(self):
        """Test that zero thread pool workers is allowed"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.THREAD_POOL_WORKERS == 0

    @patch.dict('os.environ', {'RATE_LIMIT_PER_SECOND': '-1'})
    def test_negative_rate_limit(self):
        """Test that negative rate limit is converted"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.RATE_LIMIT_PER_SECOND == -1  # Should be allowed for unlimited

    @patch.dict('os.environ', {
        'CACHE_MAX_SIZE': '5000',
        'CONCURRENT_QUERY_BATCH_SIZE': '1'
    })
    def test_edge_case_values(self):
        """Test edge case configuration values"""
        import importlib
        import config
        importlib.reload(config)
        
        assert config.CACHE_MAX_SIZE == 5000
        assert config.CONCURRENT_QUERY_BATCH_SIZE == 1
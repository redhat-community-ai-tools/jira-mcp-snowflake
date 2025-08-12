import json
import time
import logging
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from typing import Any, List, Dict, Optional, Tuple

import httpx
from cachetools import TTLCache
from asyncio_throttle import Throttler

from config import (
    SNOWFLAKE_BASE_URL,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TOKEN,
    SNOWFLAKE_WAREHOUSE,
    ENABLE_CACHING,
    CACHE_TTL_SECONDS,
    CACHE_MAX_SIZE,
    MAX_HTTP_CONNECTIONS,
    HTTP_TIMEOUT_SECONDS,
    THREAD_POOL_WORKERS,
    RATE_LIMIT_PER_SECOND,
    CONCURRENT_QUERY_BATCH_SIZE
)
from metrics import track_snowflake_query

logger = logging.getLogger(__name__)

# Global connection pool and cache
_connection_pool = None
_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL_SECONDS) if ENABLE_CACHING else None
_cache_lock = threading.RLock()
_throttler = Throttler(rate_limit=RATE_LIMIT_PER_SECOND, period=1.0)
_thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS, thread_name_prefix="snowflake-worker")


class SnowflakeConnectionPool:
    """Connection pool for Snowflake API requests"""

    def __init__(self, max_connections: int = MAX_HTTP_CONNECTIONS, timeout: float = HTTP_TIMEOUT_SECONDS):
        self.max_connections = max_connections
        self.timeout = timeout
        self._client = None
        self._lock = asyncio.Lock()

    async def get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client with connection pooling"""
        async with self._lock:
            if self._client is None or self._client.is_closed:
                limits = httpx.Limits(
                    max_keepalive_connections=self.max_connections,
                    max_connections=self.max_connections * 2,
                    keepalive_expiry=30.0
                )
                self._client = httpx.AsyncClient(
                    timeout=self.timeout,
                    http2=True,
                    limits=limits,
                    transport=httpx.AsyncHTTPTransport(
                        retries=3,
                        verify=True
                    )
                )
                logger.info(f"Created new HTTP client with {self.max_connections} max connections")
            return self._client

    async def close(self):
        """Close the connection pool"""
        async with self._lock:
            if self._client and not self._client.is_closed:
                await self._client.aclose()
                logger.info("Closed HTTP connection pool")


def get_connection_pool() -> SnowflakeConnectionPool:
    """Get the global connection pool instance"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = SnowflakeConnectionPool()
    return _connection_pool


def get_cache_key(operation: str, **kwargs) -> str:
    """Generate a cache key for the given operation and parameters"""
    key_parts = [operation]
    for k, v in sorted(kwargs.items()):
        if v is not None:
            key_parts.append(f"{k}:{v}")
    return ":".join(key_parts)


def get_from_cache(key: str) -> Optional[Any]:
    """Get value from cache thread-safely"""
    if not ENABLE_CACHING or _cache is None:
        return None
    with _cache_lock:
        return _cache.get(key)


def set_in_cache(key: str, value: Any) -> None:
    """Set value in cache thread-safely"""
    if not ENABLE_CACHING or _cache is None:
        return
    with _cache_lock:
        _cache[key] = value


def clear_cache() -> None:
    """Clear the entire cache"""
    if not ENABLE_CACHING or _cache is None:
        return
    with _cache_lock:
        _cache.clear()
        logger.info("Cache cleared")


async def cleanup_resources():
    """Cleanup global resources"""
    if _connection_pool:
        await _connection_pool.close()
    if _thread_pool:
        _thread_pool.shutdown(wait=True)
    clear_cache()


def sanitize_sql_value(value: str) -> str:
    """Sanitize a SQL value to prevent injection attacks"""
    if not isinstance(value, str):
        return str(value)
    # Remove or escape dangerous characters
    # For string values, we'll escape single quotes by doubling them
    return value.replace("'", "''")


async def make_snowflake_request(
    endpoint: str,
    method: str = "POST",
    data: dict[str, Any] = None,
    snowflake_token: Optional[str] = None,
    use_cache: bool = True
) -> dict[str, Any] | None:
    """Make a request to Snowflake API with connection pooling and caching"""
    # Use provided token or fall back to config
    token = snowflake_token or SNOWFLAKE_TOKEN

    if not token:
        logger.error("SNOWFLAKE_TOKEN environment variable is required but not set")
        return None

    # Generate cache key for GET requests
    cache_key = None
    if use_cache and method.upper() == "GET":
        cache_key = get_cache_key("api_request", endpoint=endpoint, data=str(data))
        cached_result = get_from_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for {endpoint}")
            return cached_result

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    url = f"{SNOWFLAKE_BASE_URL}/{endpoint}"

    try:
        # Use throttling to avoid overwhelming the API
        async with _throttler:
            pool = get_connection_pool()
            client = await pool.get_client()

            if method.upper() == "GET":
                response = await client.request(method, url, headers=headers, params=data)
            else:
                response = await client.request(method, url, headers=headers, json=data)

            response.raise_for_status()

            # Try to parse JSON, but handle cases where response is not valid JSON
            try:
                result = response.json()

                # Cache successful GET requests
                if use_cache and cache_key and method.upper() == "GET":
                    set_in_cache(cache_key, result)
                    logger.debug(f"Cached result for {endpoint}")

                return result
            except json.JSONDecodeError as json_error:
                logger.error(f"Failed to parse JSON response from Snowflake API: {json_error}")
                logger.error(f"Response content: {response.text[:500]}...")  # Log first 500 chars
                # Return None to indicate error, which will be handled by calling functions
                return None

    except httpx.HTTPStatusError as http_error:
        logger.error(f"HTTP error from Snowflake API: {http_error.response.status_code} - {http_error.response.text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in Snowflake API request: {str(e)}")
        return None


async def execute_snowflake_query(
    sql: str,
    snowflake_token: Optional[str] = None,
    use_cache: bool = True
) -> List[Dict[str, Any]]:
    """Execute a SQL query against Snowflake and return results with caching"""
    start_time = time.time()
    success = False

    # Check cache for SELECT queries
    cache_key = None
    if use_cache and sql.strip().upper().startswith('SELECT'):
        cache_key = get_cache_key("sql_query", sql=sql)
        cached_result = get_from_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for SQL query: {sql[:50]}...")
            track_snowflake_query(start_time, True)
            return cached_result

    try:
        # Use the statements endpoint to execute SQL
        endpoint = "statements"
        payload = {
            "statement": sql,
            "timeout": 60,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
            "warehouse": SNOWFLAKE_WAREHOUSE,
        }

        logger.info(f"Executing Snowflake query: {sql[:100]}...")  # Log first 100 chars of query

        response = await make_snowflake_request(endpoint, "POST", payload, snowflake_token)

        # Check if response is None (indicating an error in API request or JSON parsing)
        if response is None:
            logger.error("Failed to get valid response from Snowflake API")
            return []

        # Parse the response to extract data
        if response and "data" in response:
            logger.info(f"Successfully got {len(response['data'])} rows from Snowflake")

            all_data = response["data"]

            # Check for pagination/partitions
            metadata = response.get('resultSetMetaData', {})
            partition_info = metadata.get('partitionInfo', [])

            if len(partition_info) > 1:
                logger.info(f"Found {len(partition_info)} partitions, fetching remaining data...")

                # Get the statement handle for pagination
                statement_handle = response.get('statementHandle')
                if statement_handle:
                    # Fetch remaining partitions
                    for partition_index in range(1, len(partition_info)):
                        try:
                            partition_endpoint = f"statements/{statement_handle}?partition={partition_index}"
                            partition_response = await make_snowflake_request(
                                partition_endpoint, "GET", None, snowflake_token
                            )

                            if partition_response and "data" in partition_response:
                                partition_data = partition_response["data"]
                                logger.info(f"Fetched partition {partition_index}: {len(partition_data)} rows")
                                all_data.extend(partition_data)
                            else:
                                logger.warning(f"Failed to fetch partition {partition_index}")

                        except Exception as e:
                            logger.error(f"Error fetching partition {partition_index}: {e}")

                logger.info(f"Total rows after fetching all partitions: {len(all_data)}")

            success = True

            # Cache successful SELECT results
            if use_cache and cache_key:
                set_in_cache(cache_key, all_data)
                logger.debug(f"Cached SQL result: {sql[:50]}...")

            return all_data
        elif response and "resultSet" in response:
            # Handle different response formats
            result_set = response["resultSet"]
            if "data" in result_set:
                logger.info(f"Successfully got {len(result_set['data'])} rows from Snowflake (resultSet format)")
                success = True
                result_data = result_set["data"]

                # Cache successful SELECT results
                if use_cache and cache_key:
                    set_in_cache(cache_key, result_data)
                    logger.debug(f"Cached SQL result: {sql[:50]}...")

                return result_data

        logger.warning("No data found in Snowflake response")
        success = True  # No data is still a successful query
        return []

    except Exception as e:
        logger.error(f"Error executing Snowflake query: {str(e)}")
        logger.error(f"Query that failed: {sql}")
        return []
    finally:
        track_snowflake_query(start_time, success)


def parse_snowflake_timestamp(timestamp_str: str) -> str:
    """Parse Snowflake timestamp format and convert to ISO format"""
    if not timestamp_str or not isinstance(timestamp_str, str):
        return timestamp_str

    try:
        # Handle format like "1753767533.658000000 1440"
        parts = timestamp_str.strip().split()
        if len(parts) >= 2:
            timestamp_part = parts[0]
            timezone_offset_minutes = int(parts[1])

            # Convert to float to handle decimal seconds
            timestamp_float = float(timestamp_part)

            # Create datetime from timestamp
            dt = datetime.fromtimestamp(timestamp_float, tz=timezone.utc)

            # Apply timezone offset (offset is in minutes)
            offset_timedelta = timedelta(minutes=timezone_offset_minutes)
            dt_with_offset = dt + offset_timedelta

            # Return in ISO format
            return dt_with_offset.isoformat()
        else:
            # Try parsing as simple timestamp
            timestamp_float = float(timestamp_str)
            dt = datetime.fromtimestamp(timestamp_float, tz=timezone.utc)
            return dt.isoformat()

    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse timestamp '{timestamp_str}': {e}")
        return timestamp_str


def format_snowflake_row(row_data: List[Any], columns: List[str]) -> Dict[str, Any]:
    """Convert Snowflake row data to dictionary using column names"""
    if len(row_data) != len(columns):
        return {}

    result = {}
    # Date/time columns that should be parsed
    timestamp_columns = {
        'CREATED', 'UPDATED', 'DUEDATE', 'RESOLUTIONDATE',
        'ARCHIVEDDATE', '_FIVETRAN_SYNCED'
    }

    for i in range(len(columns)):
        column_name = columns[i].upper()
        value = row_data[i]

        # Parse timestamp columns
        if column_name in timestamp_columns and value:
            result[columns[i]] = parse_snowflake_timestamp(str(value))
        else:
            result[columns[i]] = value

    return result


async def format_snowflake_rows_concurrent(
    rows: List[List[Any]],
    columns: List[str],
    batch_size: int = 100
) -> List[Dict[str, Any]]:
    """Format multiple Snowflake rows concurrently using thread pool for CPU-intensive work"""
    if not rows:
        return []

    logger.debug(f"Formatting {len(rows)} rows with batch size {batch_size}")

    # For small datasets, process directly
    if len(rows) <= batch_size:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            _thread_pool,
            _format_rows_batch,
            rows,
            columns
        )

    # For large datasets, process in batches
    all_formatted = []
    tasks = []

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(_thread_pool, _format_rows_batch, batch, columns)
        tasks.append(task)

    # Execute all batches concurrently
    try:
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Error formatting batch: {result}")
            else:
                all_formatted.extend(result)

    except Exception as e:
        logger.error(f"Error in concurrent row formatting: {e}")
        # Fallback to sequential processing
        for row in rows:
            all_formatted.append(format_snowflake_row(row, columns))

    logger.debug(f"Formatted {len(all_formatted)} rows")
    return all_formatted


def _format_rows_batch(rows: List[List[Any]], columns: List[str]) -> List[Dict[str, Any]]:
    """Format a batch of rows in a thread (CPU-intensive operation)"""
    return [format_snowflake_row(row, columns) for row in rows]


async def get_issue_labels(issue_ids: List[str], snowflake_token: Optional[str] = None, use_cache: bool = True) -> Dict[str, List[str]]:
    """Get labels for given issue IDs from Snowflake with caching"""
    if not issue_ids:
        return {}

    # Check cache first
    cache_key = get_cache_key("labels", issue_ids=",".join(sorted(issue_ids)))
    if use_cache:
        cached_result = get_from_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for labels: {len(issue_ids)} issues")
            return cached_result

    labels_data = {}

    try:
        # Sanitize and validate issue IDs (should be numeric)
        sanitized_ids = []
        for issue_id in issue_ids:
            # Ensure issue IDs are numeric to prevent injection
            if isinstance(issue_id, (str, int)) and str(issue_id).isdigit():
                sanitized_ids.append(str(issue_id))

        if not sanitized_ids:
            return {}

        # Create comma-separated list for IN clause
        ids_str = "'" + "','".join(sanitized_ids) + "'"

        sql = f"""
        SELECT ISSUE, LABEL
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_LABEL_RHAI
        WHERE ISSUE IN ({ids_str}) AND LABEL IS NOT NULL
        """

        rows = await execute_snowflake_query(sql, snowflake_token, use_cache)
        columns = ["ISSUE", "LABEL"]

        for row in rows:
            row_dict = format_snowflake_row(row, columns)
            issue_id = str(row_dict.get("ISSUE"))
            label = row_dict.get("LABEL")

            if issue_id and label:
                if issue_id not in labels_data:
                    labels_data[issue_id] = []
                labels_data[issue_id].append(label)

        # Cache the result
        if use_cache:
            set_in_cache(cache_key, labels_data)
            logger.debug(f"Cached labels for {len(issue_ids)} issues")

    except Exception as e:
        logger.error(f"Error fetching labels: {str(e)}")

    return labels_data


async def get_issue_comments(issue_ids: List[str], snowflake_token: Optional[str] = None, use_cache: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    """Get comments for given issue IDs from Snowflake with caching"""
    if not issue_ids:
        return {}

    # Check cache first
    cache_key = get_cache_key("comments", issue_ids=",".join(sorted(issue_ids)))
    if use_cache:
        cached_result = get_from_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for comments: {len(issue_ids)} issues")
            return cached_result

    comments_data = {}

    try:
        # Sanitize and validate issue IDs (should be numeric)
        sanitized_ids = []
        for issue_id in issue_ids:
            # Ensure issue IDs are numeric to prevent injection
            if isinstance(issue_id, (str, int)) and str(issue_id).isdigit():
                sanitized_ids.append(str(issue_id))

        if not sanitized_ids:
            return {}

        # Create comma-separated list for IN clause
        ids_str = "'" + "','".join(sanitized_ids) + "'"

        sql = f"""
        SELECT ID, ISSUEID, ROLELEVEL, BODY, CREATED, UPDATED
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_COMMENT_NON_PII
        WHERE ISSUEID IN ({ids_str}) AND BODY IS NOT NULL
        ORDER BY ISSUEID, CREATED ASC
        """

        rows = await execute_snowflake_query(sql, snowflake_token, use_cache)
        columns = ["ID", "ISSUEID", "ROLELEVEL", "BODY", "CREATED", "UPDATED"]

        for row in rows:
            row_dict = format_snowflake_row(row, columns)
            issue_id = str(row_dict.get("ISSUEID"))

            if issue_id:
                if issue_id not in comments_data:
                    comments_data[issue_id] = []

                comment = {
                    "id": row_dict.get("ID"),
                    "role_level": row_dict.get("ROLELEVEL"),
                    "body": row_dict.get("BODY"),
                    "created": row_dict.get("CREATED"),
                    "updated": row_dict.get("UPDATED")
                }
                comments_data[issue_id].append(comment)

        # Cache the result
        if use_cache:
            set_in_cache(cache_key, comments_data)
            logger.debug(f"Cached comments for {len(issue_ids)} issues")

    except Exception as e:
        logger.error(f"Error fetching comments: {str(e)}")

    return comments_data


async def get_issue_links(issue_ids: List[str], snowflake_token: Optional[str] = None, use_cache: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    """Get issue links for given issue IDs from Snowflake with caching"""
    if not issue_ids:
        return {}

    # Check cache first
    cache_key = get_cache_key("links", issue_ids=",".join(sorted(issue_ids)))
    if use_cache:
        cached_result = get_from_cache(cache_key)
        if cached_result is not None:
            logger.debug(f"Cache hit for links: {len(issue_ids)} issues")
            return cached_result

    links_data = {}

    try:
        # Sanitize and validate issue IDs (should be numeric)
        sanitized_ids = []
        for issue_id in issue_ids:
            # Ensure issue IDs are numeric to prevent injection
            if isinstance(issue_id, (str, int)) and str(issue_id).isdigit():
                sanitized_ids.append(str(issue_id))

        if not sanitized_ids:
            return {}

        # Create comma-separated list for IN clause
        ids_str = "'" + "','".join(sanitized_ids) + "'"

        sql = f"""
        SELECT
            il.ID as LINK_ID,
            il.SOURCE,
            il.DESTINATION,
            il.SEQUENCE,
            ilt.LINKNAME,
            ilt.INWARD,
            ilt.OUTWARD,
            si.ISSUE_KEY as SOURCE_KEY,
            di.ISSUE_KEY as DESTINATION_KEY,
            si.SUMMARY as SOURCE_SUMMARY,
            di.SUMMARY as DESTINATION_SUMMARY
        FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_ISSUELINK_RHAI il
        JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_ISSUELINKTYPE_RHAI ilt
            ON il.LINKTYPE = ilt.ID
        LEFT JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_ISSUE_NON_PII si
            ON il.SOURCE = si.ID
        LEFT JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.JIRA_ISSUE_NON_PII di
            ON il.DESTINATION = di.ID
        WHERE (il.SOURCE IN ({ids_str}) OR il.DESTINATION IN ({ids_str}))
        ORDER BY il.SOURCE, il.SEQUENCE
        """

        rows = await execute_snowflake_query(sql, snowflake_token, use_cache)
        columns = [
            "LINK_ID", "SOURCE", "DESTINATION", "SEQUENCE", "LINKNAME",
            "INWARD", "OUTWARD", "SOURCE_KEY", "DESTINATION_KEY",
            "SOURCE_SUMMARY", "DESTINATION_SUMMARY"
        ]

        for row in rows:
            row_dict = format_snowflake_row(row, columns)
            source_id = str(row_dict.get("SOURCE"))
            destination_id = str(row_dict.get("DESTINATION"))

            # Create link object
            link = {
                "link_id": row_dict.get("LINK_ID"),
                "source_id": source_id,
                "destination_id": destination_id,
                "sequence": row_dict.get("SEQUENCE"),
                "link_type": row_dict.get("LINKNAME"),
                "inward_description": row_dict.get("INWARD"),
                "outward_description": row_dict.get("OUTWARD"),
                "source_key": row_dict.get("SOURCE_KEY"),
                "destination_key": row_dict.get("DESTINATION_KEY"),
                "source_summary": row_dict.get("SOURCE_SUMMARY"),
                "destination_summary": row_dict.get("DESTINATION_SUMMARY")
            }

            # Add to both source and destination issue data
            for issue_id in [source_id, destination_id]:
                if issue_id in sanitized_ids:
                    if issue_id not in links_data:
                        links_data[issue_id] = []

                    # Determine relationship direction for this issue
                    if issue_id == source_id:
                        link_copy = link.copy()
                        link_copy["relationship"] = "outward"
                        link_copy["related_issue_id"] = destination_id
                        link_copy["related_issue_key"] = row_dict.get("DESTINATION_KEY")
                        link_copy["related_issue_summary"] = row_dict.get("DESTINATION_SUMMARY")
                        link_copy["relationship_description"] = row_dict.get("OUTWARD")
                    else:
                        link_copy = link.copy()
                        link_copy["relationship"] = "inward"
                        link_copy["related_issue_id"] = source_id
                        link_copy["related_issue_key"] = row_dict.get("SOURCE_KEY")
                        link_copy["related_issue_summary"] = row_dict.get("SOURCE_SUMMARY")
                        link_copy["relationship_description"] = row_dict.get("INWARD")

                    links_data[issue_id].append(link_copy)

        # Cache the result
        if use_cache:
            set_in_cache(cache_key, links_data)
            logger.debug(f"Cached links for {len(issue_ids)} issues")

    except Exception as e:
        logger.error(f"Error fetching issue links: {str(e)}")

    return links_data


async def get_issue_enrichment_data_concurrent(
    issue_ids: List[str],
    snowflake_token: Optional[str] = None,
    use_cache: bool = True
) -> Tuple[Dict[str, List[str]], Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """Get labels, comments, and links for issues concurrently for better performance"""
    if not issue_ids:
        return {}, {}, {}

    logger.info(f"Fetching enrichment data for {len(issue_ids)} issues concurrently")

    # Use asyncio.gather to run all three operations concurrently
    try:
        labels_task = get_issue_labels(issue_ids, snowflake_token, use_cache)
        comments_task = get_issue_comments(issue_ids, snowflake_token, use_cache)
        links_task = get_issue_links(issue_ids, snowflake_token, use_cache)

        labels_data, comments_data, links_data = await asyncio.gather(
            labels_task, comments_task, links_task, return_exceptions=True
        )

        # Handle exceptions
        if isinstance(labels_data, Exception):
            logger.error(f"Error fetching labels: {labels_data}")
            labels_data = {}
        if isinstance(comments_data, Exception):
            logger.error(f"Error fetching comments: {comments_data}")
            comments_data = {}
        if isinstance(links_data, Exception):
            logger.error(f"Error fetching links: {links_data}")
            links_data = {}

        logger.info(f"Successfully fetched enrichment data for {len(issue_ids)} issues")
        return labels_data, comments_data, links_data

    except Exception as e:
        logger.error(f"Error in concurrent enrichment data fetch: {e}")
        return {}, {}, {}


async def execute_queries_in_batches(
    queries: List[str],
    snowflake_token: Optional[str] = None,
    batch_size: int = CONCURRENT_QUERY_BATCH_SIZE,
    use_cache: bool = True
) -> List[List[Dict[str, Any]]]:
    """Execute multiple SQL queries in concurrent batches"""
    if not queries:
        return []

    logger.info(f"Executing {len(queries)} queries in batches of {batch_size}")

    all_results = []

    # Process queries in batches
    for i in range(0, len(queries), batch_size):
        batch = queries[i:i + batch_size]
        logger.debug(f"Processing batch {i // batch_size + 1}: {len(batch)} queries")

        # Execute batch concurrently
        tasks = [execute_snowflake_query(sql, snowflake_token, use_cache) for sql in batch]

        try:
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle exceptions and collect results
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Query {i + j} failed: {result}")
                    all_results.append([])
                else:
                    all_results.append(result)

        except Exception as e:
            logger.error(f"Error in batch {i // batch_size + 1}: {e}")
            # Add empty results for failed batch
            all_results.extend([[]] * len(batch))

    logger.info(f"Completed {len(queries)} queries in batches")
    return all_results

import json
import time
import logging
from typing import Any, List, Dict, Optional

import httpx

from config import (
    SNOWFLAKE_BASE_URL,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TOKEN,
    SNOWFLAKE_WAREHOUSE
)
from metrics import track_snowflake_query

logger = logging.getLogger(__name__)


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
    snowflake_token: Optional[str] = None
) -> dict[str, Any] | None:
    """Make a request to Snowflake API"""
    # Use provided token or fall back to config
    token = snowflake_token or SNOWFLAKE_TOKEN

    if not token:
        logger.error("SNOWFLAKE_TOKEN environment variable is required but not set")
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    url = f"{SNOWFLAKE_BASE_URL}/{endpoint}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method.upper() == "GET":
                response = await client.request(method, url, headers=headers, params=data)
            else:
                response = await client.request(method, url, headers=headers, json=data)

            response.raise_for_status()

            # Try to parse JSON, but handle cases where response is not valid JSON
            try:
                return response.json()
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


async def execute_snowflake_query(sql: str, snowflake_token: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute a SQL query against Snowflake and return results"""
    start_time = time.time()
    success = False

    try:
        # Use the statements endpoint to execute SQL
        endpoint = "statements"
        payload = {
            "statement": sql,
            "timeout": 60,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "parameters": {
                "rows_per_resultset": 0  # 0 means no maximum row limit
            }
        }

        logger.info(f"Executing Snowflake query: {sql[:100]}...")  # Log first 100 chars of query

        response = await make_snowflake_request(endpoint, "POST", payload, snowflake_token)

        # Check if response is None (indicating an error in API request or JSON parsing)
        if response is None:
            logger.error("Failed to get valid response from Snowflake API")
            return []

        all_data = []

        # Parse the response to extract data from first partition
        if response and "data" in response:
            all_data.extend(response["data"])

            # Check for additional partitions
            statement_handle = response.get("statementHandle")
            if statement_handle:
                # Check if there are more partitions by looking at metadata
                metadata = response.get("resultSetMetaData", {})
                partition_info = metadata.get("partitionInfo", [])

                if len(partition_info) > 1:
                    logger.info(f"Found {len(partition_info)} partitions, fetching remaining partitions")

                    # Fetch remaining partitions (starting from partition 1)
                    for partition_num in range(1, len(partition_info)):
                        partition_endpoint = f"statements/{statement_handle}"
                        partition_response = await make_snowflake_request(
                            partition_endpoint,
                            "GET",
                            {"partition": partition_num},
                            snowflake_token
                        )

                        if partition_response and "data" in partition_response:
                            partition_data = partition_response["data"]
                            all_data.extend(partition_data)
                        else:
                            logger.warning(f"Failed to fetch partition {partition_num}")

            success = True
            logger.info(f"Successfully retrieved {len(all_data)} rows from Snowflake")
            return all_data

        elif response and "resultSet" in response:
            # Handle different response formats
            result_set = response["resultSet"]
            if "data" in result_set:
                logger.info(f"Successfully got {len(result_set['data'])} rows from Snowflake (resultSet format)")
                return result_set["data"]

        logger.error(f"Unexpected response format from Snowflake: {response}")
        return []

    except Exception as e:
        logger.error(f"Failed to execute Snowflake query: {e}")
        return []
    finally:
        track_snowflake_query(start_time, success)


def format_snowflake_row(row_data: List[Any], columns: List[str]) -> Dict[str, Any]:
    """Convert Snowflake row data to dictionary using column names"""
    if len(row_data) != len(columns):
        return {}

    return {columns[i]: row_data[i] for i in range(len(columns))}


async def get_issue_labels(issue_ids: List[str], snowflake_token: Optional[str] = None) -> Dict[str, List[str]]:
    """Get labels for given issue IDs from Snowflake"""
    if not issue_ids:
        return {}

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
        FROM JIRA_LABEL_RHAI
        WHERE ISSUE IN ({ids_str}) AND LABEL IS NOT NULL
        """

        rows = await execute_snowflake_query(sql, snowflake_token)
        columns = ["ISSUE", "LABEL"]

        for row in rows:
            row_dict = format_snowflake_row(row, columns)
            issue_id = str(row_dict.get("ISSUE"))
            label = row_dict.get("LABEL")

            if issue_id and label:
                if issue_id not in labels_data:
                    labels_data[issue_id] = []
                labels_data[issue_id].append(label)

    except Exception as e:
        logger.error(f"Error fetching labels: {str(e)}")

    return labels_data


async def get_issue_comments(issue_ids: List[str], snowflake_token: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Get comments for given issue IDs from Snowflake"""
    if not issue_ids:
        return {}

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
        FROM JIRA_COMMENT_NON_PII
        WHERE ISSUEID IN ({ids_str}) AND BODY IS NOT NULL
        ORDER BY ISSUEID, CREATED ASC
        """

        rows = await execute_snowflake_query(sql, snowflake_token)
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

    except Exception as e:
        logger.error(f"Error fetching comments: {str(e)}")

    return comments_data

import os
import csv
import json
from typing import Any, Optional, List, Dict
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("mcp-server")


# Path to CSV files
CSV_DIR = Path(__file__).parent / "Snowflake_CSV"


@mcp.tool()
async def list_issues(
    project: Optional[str] = None,
    issue_type: Optional[str] = None,
    status: Optional[str] = None,
    priority: Optional[str] = None,
    limit: int = 50,
    search_text: Optional[str] = None
) -> Dict[str, Any]:
    """
    List JIRA issues from CSV data with optional filtering.
    
    Args:
        project: Filter by project key (e.g., 'SMQE', 'OSIM')
        issue_type: Filter by issue type ID
        status: Filter by issue status ID
        priority: Filter by priority ID
        limit: Maximum number of issues to return (default: 50)
        search_text: Search in summary and description fields
    
    Returns:
        Dictionary containing issues list and metadata
    """
    try:
        issues = []
        issues_file = CSV_DIR / "JIRA_ISSUE_NON_PII.csv"
        
        if not issues_file.exists():
            return {"error": "JIRA issues CSV file not found", "issues": []}
        
        # Read issues from CSV
        with open(issues_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Apply filters
                if project and row.get('PROJECT', '').upper() != project.upper():
                    continue
                    
                if issue_type and row.get('ISSUETYPE') != str(issue_type):
                    continue
                    
                if status and row.get('ISSUESTATUS') != str(status):
                    continue
                    
                if priority and row.get('PRIORITY') != str(priority):
                    continue
                
                # Search in summary and description
                if search_text:
                    search_lower = search_text.lower()
                    summary = row.get('SUMMARY', '').lower()
                    description = row.get('DESCRIPTION', '').lower()
                    
                    if search_lower not in summary and search_lower not in description:
                        continue
                
                # Build issue object
                issue = {
                    "id": row.get('ID'),
                    "key": row.get('ISSUE_KEY'),
                    "project": row.get('PROJECT'),
                    "issue_number": row.get('ISSUENUM'),
                    "issue_type": row.get('ISSUETYPE'),
                    "summary": row.get('SUMMARY'),
                    "description": row.get('DESCRIPTION', '')[:500] + "..." if len(row.get('DESCRIPTION', '')) > 500 else row.get('DESCRIPTION', ''),
                    "priority": row.get('PRIORITY'),
                    "status": row.get('ISSUESTATUS'),
                    "resolution": row.get('RESOLUTION'),
                    "created": row.get('CREATED'),
                    "updated": row.get('UPDATED'),
                    "due_date": row.get('DUEDATE'),
                    "resolution_date": row.get('RESOLUTIONDATE'),
                    "votes": row.get('VOTES'),
                    "watches": row.get('WATCHES'),
                    "environment": row.get('ENVIRONMENT'),
                    "component": row.get('COMPONENT'),
                    "fix_version": row.get('FIXFOR')
                }
                
                issues.append(issue)
                
                # Apply limit
                if len(issues) >= limit:
                    break
        
        # Get labels for enrichment
        labels_data = await _get_issue_labels([issue['id'] for issue in issues])
        
        # Enrich issues with labels
        for issue in issues:
            issue_id = issue['id']
            issue['labels'] = labels_data.get(issue_id, [])
        
        return {
            "issues": issues,
            "total_returned": len(issues),
            "filters_applied": {
                "project": project,
                "issue_type": issue_type,
                "status": status,
                "priority": priority,
                "search_text": search_text,
                "limit": limit
            }
        }
        
    except Exception as e:
        return {"error": f"Error reading issues: {str(e)}", "issues": []}


async def _get_issue_labels(issue_ids: List[str]) -> Dict[str, List[str]]:
    """Get labels for given issue IDs"""
    labels_data = {}
    labels_file = CSV_DIR / "JIRA_LABEL_RHAI.csv"
    
    if not labels_file.exists():
        return labels_data
    
    try:
        with open(labels_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                issue_id = row.get('ISSUE')
                label = row.get('LABEL')
                
                if issue_id in issue_ids and label:
                    if issue_id not in labels_data:
                        labels_data[issue_id] = []
                    labels_data[issue_id].append(label)
    except Exception:
        pass
    
    return labels_data


@mcp.tool()
async def get_issue_details(issue_key: str) -> Dict[str, Any]:
    """
    Get detailed information for a specific JIRA issue by its key.
    
    Args:
        issue_key: The JIRA issue key (e.g., 'SMQE-1280')
    
    Returns:
        Dictionary containing detailed issue information
    """
    try:
        issues_file = CSV_DIR / "JIRA_ISSUE_NON_PII.csv"
        
        if not issues_file.exists():
            return {"error": "JIRA issues CSV file not found"}
        
        # Find the issue
        with open(issues_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                if row.get('ISSUE_KEY') == issue_key:
                    issue = {
                        "id": row.get('ID'),
                        "key": row.get('ISSUE_KEY'),
                        "project": row.get('PROJECT'),
                        "issue_number": row.get('ISSUENUM'),
                        "issue_type": row.get('ISSUETYPE'),
                        "summary": row.get('SUMMARY'),
                        "description": row.get('DESCRIPTION', ''),
                        "priority": row.get('PRIORITY'),
                        "status": row.get('ISSUESTATUS'),
                        "resolution": row.get('RESOLUTION'),
                        "created": row.get('CREATED'),
                        "updated": row.get('UPDATED'),
                        "due_date": row.get('DUEDATE'),
                        "resolution_date": row.get('RESOLUTIONDATE'),
                        "votes": row.get('VOTES'),
                        "watches": row.get('WATCHES'),
                        "environment": row.get('ENVIRONMENT'),
                        "component": row.get('COMPONENT'),
                        "fix_version": row.get('FIXFOR'),
                        "time_original_estimate": row.get('TIMEORIGINALESTIMATE'),
                        "time_estimate": row.get('TIMEESTIMATE'),
                        "time_spent": row.get('TIMESPENT'),
                        "workflow_id": row.get('WORKFLOW_ID'),
                        "security": row.get('SECURITY'),
                        "archived": row.get('ARCHIVED'),
                        "archived_date": row.get('ARCHIVEDDATE')
                    }
                    
                    # Get labels for this issue
                    labels_data = await _get_issue_labels([issue['id']])
                    issue['labels'] = labels_data.get(issue['id'], [])
                    
                    return {"issue": issue}
        
        return {"error": f"Issue with key '{issue_key}' not found"}
        
    except Exception as e:
        return {"error": f"Error retrieving issue details: {str(e)}"}


@mcp.tool()
async def get_project_summary() -> Dict[str, Any]:
    """
    Get a summary of all projects in the JIRA data.
    
    Returns:
        Dictionary containing project statistics
    """
    try:
        issues_file = CSV_DIR / "JIRA_ISSUE_NON_PII.csv"
        
        if not issues_file.exists():
            return {"error": "JIRA issues CSV file not found"}
        
        project_stats = {}
        total_issues = 0
        
        with open(issues_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                project = row.get('PROJECT', 'Unknown')
                status = row.get('ISSUESTATUS', 'Unknown')
                priority = row.get('PRIORITY', 'Unknown')
                
                if project not in project_stats:
                    project_stats[project] = {
                        'total_issues': 0,
                        'statuses': {},
                        'priorities': {}
                    }
                
                project_stats[project]['total_issues'] += 1
                project_stats[project]['statuses'][status] = project_stats[project]['statuses'].get(status, 0) + 1
                project_stats[project]['priorities'][priority] = project_stats[project]['priorities'].get(priority, 0) + 1
                
                total_issues += 1
        
        return {
            "total_issues": total_issues,
            "total_projects": len(project_stats),
            "projects": project_stats
        }
        
    except Exception as e:
        return {"error": f"Error generating project summary: {str(e)}"}


if __name__ == "__main__":
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "stdio"))

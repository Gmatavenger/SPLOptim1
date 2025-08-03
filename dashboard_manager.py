"""
Dashboard Management Module
==========================

This module handles all dashboard and list management operations.
It provides:
- Dashboard CRUD operations with validation
- List (formerly groups) management with multi-selection support
- Data persistence and backup
- Search and filtering capabilities
- Import/export functionality
"""

import json
import os
import logging
import uuid
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
from urllib.parse import urlparse

from utils.config import Config, get_current_timestamp, validate_url, sanitize_filename

logger = logging.getLogger(__name__)

class DashboardManager:
    """
    Manages dashboard and list operations for the Splunk Dashboard Automator.
    
    This class handles:
    - Dashboard creation, modification, and deletion
    - List management with multi-assignment support
    - Data validation and sanitization
    - Persistent storage with backup
    - Search and filtering operations
    """
    
    def __init__(self):
        """Initialize the dashboard manager."""
        self.dashboards = {}
        self.lists = set()
        
        # Load existing data
        self.load_dashboards()
        
        logger.info("Dashboard manager initialized")
    
    def generate_id(self) -> str:
        """
        Generate a unique ID for a new dashboard.
        
        Returns:
            str: Unique dashboard ID
        """
        return str(uuid.uuid4())
    
    def add_dashboard(self, dashboard_data: Dict[str, Any]) -> bool:
        """
        Add a new dashboard to the system.
        
        Args:
            dashboard_data (Dict[str, Any]): Dashboard configuration
            
        Returns:
            bool: True if dashboard was added successfully, False otherwise
        """
        try:
            # Validate dashboard data
            validation_result = self._validate_dashboard(dashboard_data)
            if not validation_result['valid']:
                logger.error(f"Invalid dashboard data: {validation_result['error']}")
                return False
            
            # Generate ID if not provided
            if 'id' not in dashboard_data:
                dashboard_data['id'] = self.generate_id()
            
            # Sanitize and prepare dashboard object
            dashboard = {
                'id': dashboard_data['id'],
                'name': dashboard_data['name'].strip(),
                'url': dashboard_data['url'].strip(),
                'lists': dashboard_data.get('lists', []),
                'description': dashboard_data.get('description', '').strip(),
                'selected': False,
                'status': 'Ready',
                'created_at': dashboard_data.get('created_at', get_current_timestamp()),
                'updated_at': get_current_timestamp(),
                'last_captured': None,
                'capture_count': 0,
                'metadata': dashboard_data.get('metadata', {})
            }
            
            # Add lists to the global lists set
            for list_name in dashboard['lists']:
                self.lists.add(list_name)
            
            # Add to dashboards dictionary
            self.dashboards[dashboard['id']] = dashboard
            
            # Save to file
            self.save_dashboards()
            
            logger.info(f"Added dashboard: {dashboard['name']} (ID: {dashboard['id']})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding dashboard: {e}")
            return False
    
    def update_dashboard(self, dashboard_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an existing dashboard.
        
        Args:
            dashboard_id (str): ID of the dashboard to update
            update_data (Dict[str, Any]): Fields to update
            
        Returns:
            bool: True if dashboard was updated successfully, False otherwise
        """
        try:
            if dashboard_id not in self.dashboards:
                logger.error(f"Dashboard not found: {dashboard_id}")
                return False
            
            dashboard = self.dashboards[dashboard_id]
            
            # Validate update data if name or URL are being changed
            if 'name' in update_data or 'url' in update_data:
                temp_data = dashboard.copy()
                temp_data.update(update_data)
                validation_result = self._validate_dashboard(temp_data, dashboard_id)
                if not validation_result['valid']:
                    logger.error(f"Invalid update data: {validation_result['error']}")
                    return False
            
            # Update fields
            for key, value in update_data.items():
                if key in ['id', 'created_at', 'capture_count']:
                    continue  # Don't allow updating these fields
                
                if key == 'name' or key == 'description':
                    dashboard[key] = value.strip() if isinstance(value, str) else value
                elif key == 'url':
                    dashboard[key] = value.strip() if isinstance(value, str) else value
                elif key == 'lists':
                    # Handle list updates
                    old_lists = set(dashboard.get('lists', []))
                    new_lists = set(value if isinstance(value, list) else [])
                    
                    dashboard[key] = list(new_lists)
                    
                    # Update global lists set
                    for list_name in new_lists:
                        self.lists.add(list_name)
                    
                    # Remove unused lists (if no other dashboards use them)
                    self._cleanup_unused_lists()
                else:
                    dashboard[key] = value
            
            # Update timestamp
            dashboard['updated_at'] = get_current_timestamp()
            
            # Save to file
            self.save_dashboards()
            
            logger.info(f"Updated dashboard: {dashboard['name']} (ID: {dashboard_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error updating dashboard: {e}")
            return False
    
    def delete_dashboard(self, dashboard_id: str) -> bool:
        """
        Delete a single dashboard from the system.
        
        Args:
            dashboard_id (str): ID of the dashboard to delete
            
        Returns:
            bool: True if dashboard was deleted successfully, False otherwise
        """
        return self.delete_dashboards([dashboard_id])
    
    def delete_dashboards(self, dashboard_ids: List[str]) -> bool:
        """
        Delete multiple dashboards from the system.
        
        Args:
            dashboard_ids (List[str]): List of dashboard IDs to delete
            
        Returns:
            bool: True if all dashboards were deleted successfully, False otherwise
        """
        try:
            if not dashboard_ids:
                logger.error("No dashboard IDs provided for deletion")
                return False
            
            deleted_names = []
            
            for dashboard_id in dashboard_ids:
                if dashboard_id in self.dashboards:
                    dashboard_name = self.dashboards[dashboard_id]['name']
                    del self.dashboards[dashboard_id]
                    deleted_names.append(dashboard_name)
                else:
                    logger.warning(f"Dashboard not found for deletion: {dashboard_id}")
            
            if deleted_names:
                # Clean up unused lists
                self._cleanup_unused_lists()
                
                # Save to file
                self.save_dashboards()
                
                logger.info(f"Deleted dashboards: {', '.join(deleted_names)}")
                return True
            else:
                logger.error("No valid dashboards found for deletion")
                return False
            
        except Exception as e:
            logger.error(f"Error deleting dashboards: {e}")
            return False
    
    def get_dashboard(self, dashboard_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific dashboard by ID.
        
        Args:
            dashboard_id (str): Dashboard ID
            
        Returns:
            Optional[Dict[str, Any]]: Dashboard data if found, None otherwise
        """
        return self.dashboards.get(dashboard_id)
    
    def get_all_dashboards(self) -> List[Dict[str, Any]]:
        """
        Get all dashboards in the system.
        
        Returns:
            List[Dict[str, Any]]: List of all dashboards
        """
        return list(self.dashboards.values())
    
    def get_dashboards_by_ids(self, dashboard_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get multiple dashboards by their IDs.
        
        Args:
            dashboard_ids (List[str]): List of dashboard IDs
            
        Returns:
            List[Dict[str, Any]]: List of found dashboards
        """
        dashboards = []
        for dashboard_id in dashboard_ids:
            dashboard = self.get_dashboard(dashboard_id)
            if dashboard:
                dashboards.append(dashboard)
            else:
                logger.warning(f"Dashboard not found: {dashboard_id}")
        
        return dashboards
    
    def get_dashboards_by_list(self, list_name: str) -> List[Dict[str, Any]]:
        """
        Get all dashboards assigned to a specific list.
        
        Args:
            list_name (str): Name of the list
            
        Returns:
            List[Dict[str, Any]]: List of dashboards in the specified list
        """
        return [
            dashboard for dashboard in self.dashboards.values()
            if list_name in dashboard.get('lists', [])
        ]
    
    def search_dashboards(self, query: str) -> List[Dict[str, Any]]:
        """
        Search dashboards by name, URL, or description.
        
        Args:
            query (str): Search query
            
        Returns:
            List[Dict[str, Any]]: List of matching dashboards
        """
        query_lower = query.lower().strip()
        if not query_lower:
            return self.get_all_dashboards()
        
        matching_dashboards = []
        
        for dashboard in self.dashboards.values():
            # Search in name
            if query_lower in dashboard.get('name', '').lower():
                matching_dashboards.append(dashboard)
                continue
            
            # Search in URL
            if query_lower in dashboard.get('url', '').lower():
                matching_dashboards.append(dashboard)
                continue
            
            # Search in description
            if query_lower in dashboard.get('description', '').lower():
                matching_dashboards.append(dashboard)
                continue
            
            # Search in lists
            for list_name in dashboard.get('lists', []):
                if query_lower in list_name.lower():
                    matching_dashboards.append(dashboard)
                    break
        
        return matching_dashboards
    
    def add_list(self, list_name: str) -> bool:
        """
        Add a new list to the system.
        
        Args:
            list_name (str): Name of the list to add
            
        Returns:
            bool: True if list was added successfully, False if it already exists
        """
        list_name = list_name.strip()
        
        if not list_name:
            logger.error("List name cannot be empty")
            return False
        
        if list_name in self.lists:
            logger.error(f"List already exists: {list_name}")
            return False
        
        self.lists.add(list_name)
        self.save_dashboards()  # Lists are saved with dashboards
        
        logger.info(f"Added list: {list_name}")
        return True
    
    def rename_list(self, old_name: str, new_name: str) -> bool:
        """
        Rename a list and update all associated dashboards.
        
        Args:
            old_name (str): Current name of the list
            new_name (str): New name for the list
            
        Returns:
            bool: True if list was renamed successfully, False otherwise
        """
        try:
            old_name = old_name.strip()
            new_name = new_name.strip()
            
            if not old_name or not new_name:
                logger.error("List names cannot be empty")
                return False
            
            if old_name not in self.lists:
                logger.error(f"List not found: {old_name}")
                return False
            
            if new_name in self.lists and new_name != old_name:
                logger.error(f"List already exists: {new_name}")
                return False
            
            # Update all dashboards that use this list
            for dashboard in self.dashboards.values():
                lists = dashboard.get('lists', [])
                if old_name in lists:
                    # Replace old name with new name
                    dashboard['lists'] = [new_name if l == old_name else l for l in lists]
                    dashboard['updated_at'] = get_current_timestamp()
            
            # Update lists set
            self.lists.remove(old_name)
            self.lists.add(new_name)
            
            # Save changes
            self.save_dashboards()
            
            logger.info(f"Renamed list: {old_name} -> {new_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error renaming list: {e}")
            return False
    
    def delete_list(self, list_name: str) -> bool:
        """
        Delete a list and remove it from all associated dashboards.
        
        Args:
            list_name (str): Name of the list to delete
            
        Returns:
            bool: True if list was deleted successfully, False otherwise
        """
        try:
            list_name = list_name.strip()
            
            if not list_name:
                logger.error("List name cannot be empty")
                return False
            
            if list_name not in self.lists:
                logger.error(f"List not found: {list_name}")
                return False
            
            # Remove from all dashboards
            for dashboard in self.dashboards.values():
                lists = dashboard.get('lists', [])
                if list_name in lists:
                    dashboard['lists'] = [l for l in lists if l != list_name]
                    dashboard['updated_at'] = get_current_timestamp()
            
            # Remove from lists set
            self.lists.remove(list_name)
            
            # Save changes
            self.save_dashboards()
            
            logger.info(f"Deleted list: {list_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting list: {e}")
            return False
    
    def get_all_lists(self) -> List[str]:
        """
        Get all lists in the system.
        
        Returns:
            List[str]: Sorted list of all list names
        """
        return sorted(list(self.lists))
    
    def get_list_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about lists and dashboard distribution.
        
        Returns:
            Dict[str, Any]: Statistics including list counts and usage
        """
        stats = {
            'total_lists': len(self.lists),
            'total_dashboards': len(self.dashboards),
            'list_usage': {},
            'dashboards_without_lists': 0,
            'most_used_list': None,
            'least_used_list': None
        }
        
        # Count dashboard usage per list
        list_counts = {}
        for list_name in self.lists:
            list_counts[list_name] = 0
        
        dashboards_without_lists = 0
        
        for dashboard in self.dashboards.values():
            dashboard_lists = dashboard.get('lists', [])
            if not dashboard_lists:
                dashboards_without_lists += 1
            else:
                for list_name in dashboard_lists:
                    if list_name in list_counts:
                        list_counts[list_name] += 1
        
        stats['list_usage'] = list_counts
        stats['dashboards_without_lists'] = dashboards_without_lists
        
        # Find most and least used lists
        if list_counts:
            most_used = max(list_counts, key=list_counts.get)
            least_used = min(list_counts, key=list_counts.get)
            
            stats['most_used_list'] = {
                'name': most_used,
                'count': list_counts[most_used]
            }
            stats['least_used_list'] = {
                'name': least_used,
                'count': list_counts[least_used]
            }
        
        return stats
    
    def export_dashboards(self, file_path: str = None) -> str:
        """
        Export all dashboards to a JSON file.
        
        Args:
            file_path (str, optional): Custom file path for export
            
        Returns:
            str: Path to the exported file
        """
        try:
            if not file_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_path = f"dashboards_export_{timestamp}.json"
            
            export_data = {
                'export_timestamp': get_current_timestamp(),
                'total_dashboards': len(self.dashboards),
                'total_lists': len(self.lists),
                'dashboards': list(self.dashboards.values()),
                'lists': list(self.lists)
            }
            
            with open(file_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Exported {len(self.dashboards)} dashboards to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error exporting dashboards: {e}")
            raise
    
    def import_dashboards(self, file_path: str, merge: bool = True) -> Dict[str, Any]:
        """
        Import dashboards from a JSON file.
        
        Args:
            file_path (str): Path to the import file
            merge (bool): If True, merge with existing data; if False, replace
            
        Returns:
            Dict[str, Any]: Import results
        """
        try:
            with open(file_path, 'r') as f:
                import_data = json.load(f)
            
            if 'dashboards' not in import_data:
                raise ValueError("Invalid import file format: missing 'dashboards' key")
            
            imported_dashboards = import_data['dashboards']
            imported_lists = import_data.get('lists', [])
            
            results = {
                'success': True,
                'imported_dashboards': 0,
                'updated_dashboards': 0,
                'skipped_dashboards': 0,
                'imported_lists': 0,
                'errors': []
            }
            
            if not merge:
                # Replace mode: clear existing data
                self.dashboards = {}
                self.lists = set()
            
            # Import lists first
            for list_name in imported_lists:
                if list_name not in self.lists:
                    self.lists.add(list_name)
                    results['imported_lists'] += 1
            
            # Import dashboards
            for dashboard_data in imported_dashboards:
                try:
                    dashboard_id = dashboard_data.get('id')
                    
                    if merge and dashboard_id in self.dashboards:
                        # Update existing dashboard
                        if self.update_dashboard(dashboard_id, dashboard_data):
                            results['updated_dashboards'] += 1
                        else:
                            results['skipped_dashboards'] += 1
                    else:
                        # Add new dashboard
                        if self.add_dashboard(dashboard_data):
                            results['imported_dashboards'] += 1
                        else:
                            results['skipped_dashboards'] += 1
                            
                except Exception as e:
                    error_msg = f"Error importing dashboard {dashboard_data.get('name', 'Unknown')}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Import completed: {results['imported_dashboards']} new, "
                       f"{results['updated_dashboards']} updated, "
                       f"{results['skipped_dashboards']} skipped")
            
            return results
            
        except Exception as e:
            logger.error(f"Error importing dashboards: {e}")
            return {
                'success': False,
                'error': str(e),
                'imported_dashboards': 0,
                'updated_dashboards': 0,
                'skipped_dashboards': 0,
                'imported_lists': 0,
                'errors': [str(e)]
            }
    
    def save_dashboards(self):
        """Save all dashboards and lists to the configuration file."""
        try:
            # Create backup of existing file
            if os.path.exists(Config.DASHBOARD_FILE):
                backup_file = f"{Config.DASHBOARD_FILE}.backup"
                shutil.copy2(Config.DASHBOARD_FILE, backup_file)
            
            # Prepare data for saving
            save_data = {
                'dashboards': self.dashboards,
                'lists': list(self.lists),
                'last_updated': get_current_timestamp(),
                'version': '1.0'
            }
            
            # Save to file
            with open(Config.DASHBOARD_FILE, 'w') as f:
                json.dump(save_data, f, indent=2, default=str)
            
            logger.debug("Dashboards and lists saved to file")
            
        except Exception as e:
            logger.error(f"Error saving dashboards: {e}")
            # Try to restore from backup
            backup_file = f"{Config.DASHBOARD_FILE}.backup"
            if os.path.exists(backup_file):
                try:
                    shutil.copy2(backup_file, Config.DASHBOARD_FILE)
                    logger.info("Restored dashboards from backup after save error")
                except Exception as restore_error:
                    logger.error(f"Failed to restore from backup: {restore_error}")
    
    def load_dashboards(self):
        """Load dashboards and lists from the configuration file."""
        try:
            if os.path.exists(Config.DASHBOARD_FILE):
                with open(Config.DASHBOARD_FILE, 'r') as f:
                    data = json.load(f)
                
                # Handle different file formats for backward compatibility
                if isinstance(data, dict) and 'dashboards' in data:
                    # New format with separate lists
                    self.dashboards = data['dashboards']
                    self.lists = set(data.get('lists', []))
                elif isinstance(data, dict):
                    # Old format where data is directly dashboards
                    self.dashboards = data
                    self.lists = set()
                    # Extract lists from dashboards
                    for dashboard in self.dashboards.values():
                        for list_name in dashboard.get('lists', []):
                            self.lists.add(list_name)
                else:
                    # Unknown format
                    logger.error("Unknown dashboard file format")
                    self.dashboards = {}
                    self.lists = set()
                
                # Ensure all dashboards have required fields
                self._migrate_dashboard_format()
                
                logger.info(f"Loaded {len(self.dashboards)} dashboards and {len(self.lists)} lists from file")
            else:
                self.dashboards = {}
                self.lists = set()
                logger.info("No existing dashboard file found, starting with empty data")
                
        except Exception as e:
            logger.error(f"Error loading dashboards: {e}")
            self.dashboards = {}
            self.lists = set()
    
    def _migrate_dashboard_format(self):
        """Ensure all dashboards have the required fields for current version."""
        current_time = get_current_timestamp()
        
        for dashboard_id, dashboard in self.dashboards.items():
            # Add missing fields with default values
            if 'id' not in dashboard:
                dashboard['id'] = dashboard_id
            if 'created_at' not in dashboard:
                dashboard['created_at'] = current_time
            if 'updated_at' not in dashboard:
                dashboard['updated_at'] = current_time
            if 'status' not in dashboard:
                dashboard['status'] = 'Ready'
            if 'selected' not in dashboard:
                dashboard['selected'] = False
            if 'lists' not in dashboard:
                dashboard['lists'] = []
            if 'description' not in dashboard:
                dashboard['description'] = ''
            if 'last_captured' not in dashboard:
                dashboard['last_captured'] = None
            if 'capture_count' not in dashboard:
                dashboard['capture_count'] = 0
            if 'metadata' not in dashboard:
                dashboard['metadata'] = {}
    
    def _validate_dashboard(self, dashboard_data: Dict[str, Any], exclude_id: str = None) -> Dict[str, Any]:
        """
        Validate dashboard data.
        
        Args:
            dashboard_data (Dict[str, Any]): Dashboard data to validate
            exclude_id (str, optional): Dashboard ID to exclude from duplicate checks
            
        Returns:
            Dict[str, Any]: Validation result with 'valid' boolean and 'error' message
        """
        try:
            # Required fields
            if 'name' not in dashboard_data or not dashboard_data['name'].strip():
                return {'valid': False, 'error': 'Dashboard name is required'}
            
            if 'url' not in dashboard_data or not dashboard_data['url'].strip():
                return {'valid': False, 'error': 'Dashboard URL is required'}
            
            # Validate URL format
            url = dashboard_data['url'].strip()
            if not validate_url(url):
                return {'valid': False, 'error': 'Invalid URL format'}
            
            # Check for duplicate names
            name = dashboard_data['name'].strip()
            for dashboard_id, dashboard in self.dashboards.items():
                if dashboard_id != exclude_id and dashboard['name'].lower() == name.lower():
                    return {'valid': False, 'error': f'Dashboard name "{name}" already exists'}
            
            # Check for duplicate URLs
            for dashboard_id, dashboard in self.dashboards.items():
                if dashboard_id != exclude_id and dashboard['url'] == url:
                    return {'valid': False, 'error': f'Dashboard URL "{url}" already exists'}
            
            # Validate lists if provided
            if 'lists' in dashboard_data:
                lists = dashboard_data['lists']
                if not isinstance(lists, list):
                    return {'valid': False, 'error': 'Lists must be a list of strings'}
                
                for list_name in lists:
                    if not isinstance(list_name, str) or not list_name.strip():
                        return {'valid': False, 'error': 'All list names must be non-empty strings'}
            
            return {'valid': True, 'error': None}
            
        except Exception as e:
            return {'valid': False, 'error': f'Validation error: {str(e)}'}
    
    def _cleanup_unused_lists(self):
        """Remove lists that are not used by any dashboard."""
        used_lists = set()
        for dashboard in self.dashboards.values():
            for list_name in dashboard.get('lists', []):
                used_lists.add(list_name)
        
        # Remove unused lists
        unused_lists = self.lists - used_lists
        for unused_list in unused_lists:
            self.lists.remove(unused_list)
            logger.debug(f"Removed unused list: {unused_list}")
    
    def update_dashboard_status(self, dashboard_id: str, status: str, 
                              last_captured: str = None) -> bool:
        """
        Update dashboard status and capture information.
        
        Args:
            dashboard_id (str): Dashboard ID
            status (str): New status
            last_captured (str, optional): Timestamp of last capture
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if dashboard_id not in self.dashboards:
                return False
            
            dashboard = self.dashboards[dashboard_id]
            dashboard['status'] = status
            dashboard['updated_at'] = get_current_timestamp()
            
            if last_captured:
                dashboard['last_captured'] = last_captured
                dashboard['capture_count'] = dashboard.get('capture_count', 0) + 1
            
            self.save_dashboards()
            return True
            
        except Exception as e:
            logger.error(f"Error updating dashboard status: {e}")
            return False


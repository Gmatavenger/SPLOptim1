"""
Scheduler Management Module
==========================

This module provides advanced scheduling functionality for automated screenshot capture.
It supports:
- Multiple concurrent schedules with different frequencies
- Persistent schedule storage and management
- Background execution with proper error handling
- Schedule validation and conflict detection
- Real-time schedule monitoring and status updates
"""

import asyncio
import threading
import time
import json
import os
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from enum import Enum

from utils.config import Config, get_current_timestamp
from utils.screenshot import ScreenshotManager
from utils.dashboard_manager import DashboardManager

logger = logging.getLogger(__name__)

class ScheduleType(Enum):
    """Enumeration of supported schedule types."""
    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class ScheduleStatus(Enum):
    """Enumeration of schedule statuses."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"

class ScheduleManager:
    """
    Manages scheduling operations for automated dashboard screenshot capture.
    
    This class handles:
    - Schedule creation, modification, and deletion
    - Background schedule execution
    - Schedule persistence and recovery
    - Concurrent schedule management
    - Error handling and retry logic
    """
    
    def __init__(self):
        """Initialize the schedule manager."""
        self.schedules = {}
        self.running = False
        self.scheduler_thread = None
        self.screenshot_manager = ScreenshotManager()
        self.dashboard_manager = None  # Will be initialized when needed
        
        # Load existing schedules
        self.load_schedules()
        
        logger.info("Schedule manager initialized")
    
    def start(self):
        """Start the background scheduler thread."""
        if not self.running:
            self.running = True
            self.scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                name="SchedulerThread",
                daemon=True
            )
            self.scheduler_thread.start()
            logger.info("Scheduler started")
    
    def stop(self):
        """Stop the background scheduler thread."""
        if self.running:
            self.running = False
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
            logger.info("Scheduler stopped")
    
    def generate_id(self) -> str:
        """
        Generate a unique ID for a new schedule.
        
        Returns:
            str: Unique schedule ID
        """
        return str(uuid.uuid4())
    
    def add_schedule(self, schedule_data: Dict[str, Any]) -> bool:
        """
        Add a new schedule to the system.
        
        Args:
            schedule_data (Dict[str, Any]): Schedule configuration
            
        Returns:
            bool: True if schedule was added successfully, False otherwise
        """
        try:
            # Validate schedule data
            validation_result = self._validate_schedule(schedule_data)
            if not validation_result['valid']:
                logger.error(f"Invalid schedule data: {validation_result['error']}")
                return False
            
            # Generate ID if not provided
            if 'id' not in schedule_data:
                schedule_data['id'] = self.generate_id()
            
            # Set default values
            schedule = {
                'id': schedule_data['id'],
                'name': schedule_data['name'],
                'dashboard_ids': schedule_data['dashboard_ids'],
                'schedule_type': schedule_data['schedule_type'],
                'schedule_time': schedule_data['schedule_time'],
                'time_range': schedule_data.get('time_range', {}),
                'include_watermark': schedule_data.get('include_watermark', True),
                'active': schedule_data.get('active', True),
                'created_at': schedule_data.get('created_at', get_current_timestamp()),
                'last_run': None,
                'next_run': None,
                'run_count': 0,
                'status': ScheduleStatus.ACTIVE.value
            }
            
            # Calculate next run time
            schedule['next_run'] = self._calculate_next_run(schedule)
            
            # Add to schedules dictionary
            self.schedules[schedule['id']] = schedule
            
            # Save to file
            self.save_schedules()
            
            logger.info(f"Added schedule: {schedule['name']} (ID: {schedule['id']})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding schedule: {e}")
            return False
    
    def update_schedule(self, schedule_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an existing schedule.
        
        Args:
            schedule_id (str): ID of the schedule to update
            update_data (Dict[str, Any]): Fields to update
            
        Returns:
            bool: True if schedule was updated successfully, False otherwise
        """
        try:
            if schedule_id not in self.schedules:
                logger.error(f"Schedule not found: {schedule_id}")
                return False
            
            schedule = self.schedules[schedule_id]
            
            # Update fields
            for key, value in update_data.items():
                if key in ['id', 'created_at', 'run_count']:
                    continue  # Don't allow updating these fields
                schedule[key] = value
            
            # Recalculate next run time if schedule details changed
            if any(key in update_data for key in ['schedule_type', 'schedule_time', 'active']):
                schedule['next_run'] = self._calculate_next_run(schedule)
            
            # Save to file
            self.save_schedules()
            
            logger.info(f"Updated schedule: {schedule['name']} (ID: {schedule_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error updating schedule: {e}")
            return False
    
    def delete_schedule(self, schedule_id: str) -> bool:
        """
        Delete a schedule from the system.
        
        Args:
            schedule_id (str): ID of the schedule to delete
            
        Returns:
            bool: True if schedule was deleted successfully, False otherwise
        """
        try:
            if schedule_id not in self.schedules:
                logger.error(f"Schedule not found: {schedule_id}")
                return False
            
            schedule_name = self.schedules[schedule_id]['name']
            del self.schedules[schedule_id]
            
            # Save to file
            self.save_schedules()
            
            logger.info(f"Deleted schedule: {schedule_name} (ID: {schedule_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting schedule: {e}")
            return False
    
    def get_all_schedules(self) -> List[Dict[str, Any]]:
        """
        Get all schedules in the system.
        
        Returns:
            List[Dict[str, Any]]: List of all schedules
        """
        return list(self.schedules.values())
    
    def get_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific schedule by ID.
        
        Args:
            schedule_id (str): Schedule ID
            
        Returns:
            Optional[Dict[str, Any]]: Schedule data if found, None otherwise
        """
        return self.schedules.get(schedule_id)
    
    def activate_schedule(self, schedule_id: str) -> bool:
        """
        Activate a schedule.
        
        Args:
            schedule_id (str): Schedule ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.update_schedule(schedule_id, {'active': True})
    
    def deactivate_schedule(self, schedule_id: str) -> bool:
        """
        Deactivate a schedule.
        
        Args:
            schedule_id (str): Schedule ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.update_schedule(schedule_id, {'active': False})
    
    def save_schedules(self):
        """Save all schedules to the configuration file."""
        try:
            with open(Config.SCHEDULE_FILE, 'w') as f:
                json.dump(self.schedules, f, indent=2, default=str)
            logger.debug("Schedules saved to file")
        except Exception as e:
            logger.error(f"Error saving schedules: {e}")
    
    def load_schedules(self):
        """Load schedules from the configuration file."""
        try:
            if os.path.exists(Config.SCHEDULE_FILE):
                with open(Config.SCHEDULE_FILE, 'r') as f:
                    self.schedules = json.load(f)
                logger.info(f"Loaded {len(self.schedules)} schedules from file")
            else:
                self.schedules = {}
                logger.info("No existing schedules file found, starting with empty schedules")
        except Exception as e:
            logger.error(f"Error loading schedules: {e}")
            self.schedules = {}
    
    def _scheduler_loop(self):
        """Main scheduler loop that runs in a background thread."""
        logger.info("Scheduler loop started")
        
        while self.running:
            try:
                # Check for schedules that need to run
                current_time = datetime.now()
                
                for schedule_id, schedule in list(self.schedules.items()):
                    if not schedule.get('active', False):
                        continue
                    
                    next_run_str = schedule.get('next_run')
                    if not next_run_str:
                        continue
                    
                    try:
                        next_run = datetime.fromisoformat(next_run_str.replace('Z', '+00:00'))
                        if next_run.tzinfo:
                            # Convert to local time if timezone aware
                            next_run = next_run.replace(tzinfo=None)
                    except Exception:
                        logger.error(f"Invalid next_run time for schedule {schedule_id}: {next_run_str}")
                        continue
                    
                    if current_time >= next_run:
                        # Schedule needs to run
                        logger.info(f"Executing scheduled task: {schedule['name']}")
                        self._execute_schedule(schedule)
                
                # Sleep for the check interval
                time.sleep(Config.SCHEDULE_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(Config.SCHEDULE_CHECK_INTERVAL)
        
        logger.info("Scheduler loop stopped")
    
    def _execute_schedule(self, schedule: Dict[str, Any]):
        """
        Execute a scheduled task.
        
        Args:
            schedule (Dict[str, Any]): Schedule to execute
        """
        try:
            # Update schedule status
            schedule['status'] = ScheduleStatus.RUNNING.value
            schedule['last_run'] = get_current_timestamp()
            self.save_schedules()
            
            # Get dashboard manager if not initialized
            if not self.dashboard_manager:
                from utils.dashboard_manager import DashboardManager
                self.dashboard_manager = DashboardManager()
            
            # Get dashboards to capture
            dashboards = self.dashboard_manager.get_dashboards_by_ids(schedule['dashboard_ids'])
            
            if not dashboards:
                logger.warning(f"No valid dashboards found for schedule {schedule['name']}")
                schedule['status'] = ScheduleStatus.ERROR.value
                self.save_schedules()
                return
            
            # Get credentials
            from utils.encryption import load_credentials
            username, password = load_credentials()
            
            if not username or not password:
                logger.error(f"No credentials available for schedule {schedule['name']}")
                schedule['status'] = ScheduleStatus.ERROR.value
                self.save_schedules()
                return
            
            # Execute screenshot capture asynchronously
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                result = loop.run_until_complete(
                    self.screenshot_manager.capture_screenshots(
                        dashboards,
                        username,
                        password,
                        schedule.get('include_watermark', True),
                        schedule.get('time_range', {})
                    )
                )
                
                # Update schedule based on result
                if result['success']:
                    schedule['status'] = ScheduleStatus.COMPLETED.value
                    logger.info(f"Schedule {schedule['name']} completed successfully")
                else:
                    schedule['status'] = ScheduleStatus.ERROR.value
                    logger.error(f"Schedule {schedule['name']} completed with errors")
                
            finally:
                loop.close()
            
            # Update run count and calculate next run
            schedule['run_count'] = schedule.get('run_count', 0) + 1
            
            # Calculate next run time
            if schedule['schedule_type'] != ScheduleType.ONCE.value:
                schedule['next_run'] = self._calculate_next_run(schedule)
            else:
                # One-time schedule, deactivate it
                schedule['active'] = False
                schedule['status'] = ScheduleStatus.COMPLETED.value
            
            self.save_schedules()
            
        except Exception as e:
            logger.error(f"Error executing schedule {schedule['name']}: {e}")
            schedule['status'] = ScheduleStatus.ERROR.value
            self.save_schedules()
    
    def _calculate_next_run(self, schedule: Dict[str, Any]) -> Optional[str]:
        """
        Calculate the next run time for a schedule.
        
        Args:
            schedule (Dict[str, Any]): Schedule configuration
            
        Returns:
            Optional[str]: ISO format string of next run time, or None if schedule is inactive
        """
        if not schedule.get('active', False):
            return None
        
        schedule_type = schedule['schedule_type']
        schedule_time_str = schedule['schedule_time']
        
        try:
            # Parse the scheduled time
            scheduled_datetime = datetime.fromisoformat(schedule_time_str.replace('Z', ''))
            current_time = datetime.now()
            
            if schedule_type == ScheduleType.ONCE.value:
                # One-time schedule
                if scheduled_datetime > current_time:
                    return scheduled_datetime.isoformat()
                else:
                    return None  # Past one-time schedule
            
            elif schedule_type == ScheduleType.DAILY.value:
                # Daily schedule
                next_run = scheduled_datetime
                while next_run <= current_time:
                    next_run += timedelta(days=1)
                return next_run.isoformat()
            
            elif schedule_type == ScheduleType.WEEKLY.value:
                # Weekly schedule
                next_run = scheduled_datetime
                while next_run <= current_time:
                    next_run += timedelta(weeks=1)
                return next_run.isoformat()
            
            elif schedule_type == ScheduleType.MONTHLY.value:
                # Monthly schedule (approximate - adds 30 days)
                next_run = scheduled_datetime
                while next_run <= current_time:
                    next_run += timedelta(days=30)
                return next_run.isoformat()
            
            else:
                logger.error(f"Unknown schedule type: {schedule_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error calculating next run time: {e}")
            return None
    
    def _validate_schedule(self, schedule_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate schedule data.
        
        Args:
            schedule_data (Dict[str, Any]): Schedule data to validate
            
        Returns:
            Dict[str, Any]: Validation result with 'valid' boolean and 'error' message
        """
        try:
            # Required fields
            required_fields = ['name', 'dashboard_ids', 'schedule_type', 'schedule_time']
            for field in required_fields:
                if field not in schedule_data or not schedule_data[field]:
                    return {'valid': False, 'error': f'Missing required field: {field}'}
            
            # Validate schedule type
            valid_types = [e.value for e in ScheduleType]
            if schedule_data['schedule_type'] not in valid_types:
                return {'valid': False, 'error': f'Invalid schedule type: {schedule_data["schedule_type"]}'}
            
            # Validate schedule time format
            try:
                datetime.fromisoformat(schedule_data['schedule_time'].replace('Z', ''))
            except ValueError:
                return {'valid': False, 'error': 'Invalid schedule time format'}
            
            # Validate dashboard IDs
            if not isinstance(schedule_data['dashboard_ids'], list) or not schedule_data['dashboard_ids']:
                return {'valid': False, 'error': 'At least one dashboard ID must be specified'}
            
            # Validate name
            name = schedule_data['name'].strip()
            if len(name) < 1:
                return {'valid': False, 'error': 'Schedule name cannot be empty'}
            
            # Check for duplicate names (excluding current schedule if updating)
            schedule_id = schedule_data.get('id')
            for existing_id, existing_schedule in self.schedules.items():
                if existing_id != schedule_id and existing_schedule['name'] == name:
                    return {'valid': False, 'error': f'Schedule name "{name}" already exists'}
            
            return {'valid': True, 'error': None}
            
        except Exception as e:
            return {'valid': False, 'error': f'Validation error: {str(e)}'}
    
    def get_schedule_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about all schedules.
        
        Returns:
            Dict[str, Any]: Schedule statistics
        """
        stats = {
            'total_schedules': len(self.schedules),
            'active_schedules': 0,
            'inactive_schedules': 0,
            'running_schedules': 0,
            'completed_schedules': 0,
            'error_schedules': 0,
            'total_runs': 0,
            'next_scheduled_run': None
        }
        
        next_runs = []
        
        for schedule in self.schedules.values():
            # Count by status
            status = schedule.get('status', ScheduleStatus.INACTIVE.value)
            if status == ScheduleStatus.ACTIVE.value and schedule.get('active', False):
                stats['active_schedules'] += 1
            elif status == ScheduleStatus.INACTIVE.value or not schedule.get('active', False):
                stats['inactive_schedules'] += 1
            elif status == ScheduleStatus.RUNNING.value:
                stats['running_schedules'] += 1
            elif status == ScheduleStatus.COMPLETED.value:
                stats['completed_schedules'] += 1
            elif status == ScheduleStatus.ERROR.value:
                stats['error_schedules'] += 1
            
            # Sum total runs
            stats['total_runs'] += schedule.get('run_count', 0)
            
            # Collect next run times
            next_run = schedule.get('next_run')
            if next_run and schedule.get('active', False):
                try:
                    next_run_dt = datetime.fromisoformat(next_run.replace('Z', ''))
                    next_runs.append(next_run_dt)
                except Exception:
                    pass
        
        # Find earliest next run
        if next_runs:
            stats['next_scheduled_run'] = min(next_runs).isoformat()
        
        return stats
    
    def cleanup_completed_schedules(self, days_to_keep: int = 30) -> int:
        """
        Clean up old completed one-time schedules.
        
        Args:
            days_to_keep (int): Number of days to keep completed schedules
            
        Returns:
            int: Number of schedules cleaned up
        """
        try:
            current_time = datetime.now()
            cutoff_time = current_time - timedelta(days=days_to_keep)
            
            schedules_to_remove = []
            
            for schedule_id, schedule in self.schedules.items():
                # Only clean up completed one-time schedules
                if (schedule.get('schedule_type') == ScheduleType.ONCE.value and
                    schedule.get('status') == ScheduleStatus.COMPLETED.value and
                    not schedule.get('active', False)):
                    
                    last_run_str = schedule.get('last_run')
                    if last_run_str:
                        try:
                            last_run = datetime.fromisoformat(last_run_str.replace('Z', ''))
                            if last_run < cutoff_time:
                                schedules_to_remove.append(schedule_id)
                        except Exception:
                            pass
            
            # Remove old schedules
            for schedule_id in schedules_to_remove:
                del self.schedules[schedule_id]
            
            if schedules_to_remove:
                self.save_schedules()
                logger.info(f"Cleaned up {len(schedules_to_remove)} old completed schedules")
            
            return len(schedules_to_remove)
            
        except Exception as e:
            logger.error(f"Error during schedule cleanup: {e}")
            return 0


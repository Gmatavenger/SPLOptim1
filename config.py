"""
Configuration Module
===================

This module contains all the configuration constants and settings for the
Splunk Dashboard Automator application. It centralizes all configuration
in one place for easy maintenance and modification.
"""

import pytz
from datetime import datetime

class Config:
    """
    Main configuration class containing all application constants.
    
    Directory Structure:
    - LOG_DIR: Directory for application logs
    - TMP_DIR: Temporary directory for screenshots and processing
    - SCREENSHOT_ARCHIVE_DIR: Archive directory for old screenshots
    
    File Names:
    - DASHBOARD_FILE: JSON file storing dashboard configurations
    - SCHEDULE_FILE: JSON file storing schedule configurations  
    - SETTINGS_FILE: JSON file storing user settings
    - SECRETS_KEY_FILE: File containing encryption key
    - SECRETS_FILE: Encrypted file containing user credentials
    
    Archive Settings:
    - DAYS_TO_KEEP_ARCHIVES: Number of days to keep archived screenshots
    
    Timezone:
    - EST: Eastern timezone for timestamp watermarks
    """
    
    # Directory paths
    LOG_DIR = "logs"
    TMP_DIR = "tmp"
    SCREENSHOT_ARCHIVE_DIR = "screenshots"
    
    # Configuration file names
    DASHBOARD_FILE = "dashboards.json"
    SCHEDULE_FILE = "schedules.json"
    SETTINGS_FILE = "settings.json"
    SECRETS_KEY_FILE = ".secrets.key"
    SECRETS_FILE = ".secrets"
    
    # Archive management
    DAYS_TO_KEEP_ARCHIVES = 7  # Keep screenshots for 7 days
    
    # Timezone for watermarks and logging
    EST = pytz.timezone("America/New_York")
    
    # Screenshot settings
    SCREENSHOT_TIMEOUT = 30  # Seconds to wait for page load
    MAX_CONCURRENT_SCREENSHOTS = 3  # Maximum concurrent browser instances
    
    # Schedule settings
    SCHEDULE_CHECK_INTERVAL = 60  # Check schedules every 60 seconds

class Theme:
    """
    Theme configuration for light and dark modes.
    
    Each theme contains color definitions for:
    - Background colors (primary, secondary, tertiary)
    - Text colors (primary, secondary, muted)
    - Accent colors for buttons and highlights
    - Border and shadow colors
    """
    
    LIGHT = {
        'name': 'Light',
        'bg_primary': '#ffffff',
        'bg_secondary': '#f8f9fa',
        'bg_tertiary': '#e9ecef',
        'text_primary': '#212529',
        'text_secondary': '#6c757d',
        'text_muted': '#adb5bd',
        'border_color': '#dee2e6',
        'accent_primary': '#0d6efd',
        'accent_secondary': '#6f42c1',
        'success_color': '#198754',
        'warning_color': '#fd7e14',
        'danger_color': '#dc3545',
        'info_color': '#0dcaf0'
    }
    
    DARK = {
        'name': 'Dark',
        'bg_primary': '#1a1d29',
        'bg_secondary': '#242632',
        'bg_tertiary': '#2e303e',
        'text_primary': '#ffffff',
        'text_secondary': '#b8bcc8',
        'text_muted': '#868e96',
        'border_color': '#3a3d4a',
        'accent_primary': '#4dabf7',
        'accent_secondary': '#9775fa',
        'success_color': '#51cf66',
        'warning_color': '#ffa726',
        'danger_color': '#ff6b6b',
        'info_color': '#22d3ee'
    }

class TimeRangePresets:
    """
    Predefined time ranges for Splunk queries.
    Each preset contains Splunk-compatible time modifiers.
    """
    
    PRESETS = {
        'last_hour': {
            'name': 'Last Hour',
            'earliest': '-1h@h',
            'latest': 'now'
        },
        'last_4_hours': {
            'name': 'Last 4 Hours',
            'earliest': '-4h@h',
            'latest': 'now'
        },
        'last_24_hours': {
            'name': 'Last 24 Hours',
            'earliest': '-24h@h',
            'latest': 'now'
        },
        'last_7_days': {
            'name': 'Last 7 Days',
            'earliest': '-7d@d',
            'latest': 'now'
        },
        'last_30_days': {
            'name': 'Last 30 Days',
            'earliest': '-30d@d',
            'latest': 'now'
        },
        'today': {
            'name': 'Today',
            'earliest': '@d',
            'latest': 'now'
        },
        'yesterday': {
            'name': 'Yesterday',
            'earliest': '-1d@d',
            'latest': '@d'
        },
        'this_week': {
            'name': 'This Week',
            'earliest': '@w0',
            'latest': 'now'
        },
        'last_week': {
            'name': 'Last Week',
            'earliest': '-1w@w0',
            'latest': '@w0'
        },
        'this_month': {
            'name': 'This Month',
            'earliest': '@mon',
            'latest': 'now'
        },
        'last_month': {
            'name': 'Last Month',
            'earliest': '-1mon@mon',
            'latest': '@mon'
        }
    }

class LoggingConfig:
    """
    Logging configuration settings.
    """
    
    # Log file settings
    MAX_BYTES = 10 * 1024 * 1024  # 10 MB per log file
    BACKUP_COUNT = 5  # Keep 5 backup files
    
    # Log format
    FORMAT = '%(asctime)s [%(levelname)s] (%(threadName)s) %(funcName)s:%(lineno)d - %(message)s'
    
    # Log levels
    LEVEL_DEBUG = 'DEBUG'
    LEVEL_INFO = 'INFO'
    LEVEL_WARNING = 'WARNING'
    LEVEL_ERROR = 'ERROR'
    LEVEL_CRITICAL = 'CRITICAL'

class SecurityConfig:
    """
    Security-related configuration settings.
    """
    
    # Encryption settings
    ENCRYPTION_ALGORITHM = 'Fernet'  # Symmetric encryption
    KEY_SIZE = 32  # 256-bit key
    
    # File permissions (Unix/Linux only)
    SECURE_FILE_PERMISSIONS = 0o600  # Read/write for owner only
    
    # Session settings
    SESSION_TIMEOUT = 24 * 60 * 60  # 24 hours in seconds
    
    # Password requirements (if implementing user management)
    MIN_PASSWORD_LENGTH = 8
    REQUIRE_SPECIAL_CHARS = True
    REQUIRE_NUMBERS = True
    REQUIRE_UPPERCASE = True

def get_current_timestamp():
    """
    Get current timestamp in EST timezone.
    
    Returns:
        str: Formatted timestamp string
    """
    return datetime.now(Config.EST).strftime("%Y-%m-%d %H:%M:%S %Z")

def validate_url(url):
    """
    Basic URL validation.
    
    Args:
        url (str): URL to validate
        
    Returns:
        bool: True if URL appears valid, False otherwise
    """
    import re
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return url_pattern.match(url) is not None

def sanitize_filename(filename):
    """
    Sanitize filename by removing or replacing invalid characters.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename safe for filesystem
    """
    import re
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    # Ensure filename is not empty
    if not filename:
        filename = 'unnamed'
    return filename

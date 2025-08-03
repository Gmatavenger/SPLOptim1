"""
Splunk Dashboard Automator - Web Application
============================================

This is the main Flask application that provides a modern web interface
for automating Splunk dashboard interactions. It includes:
- Professional light/dark theme support
- Secure credential management with encryption
- Advanced scheduling system
- Screenshot capture with enhanced watermarks
- Dashboard and list management
"""

from flask import Flask, render_template, request, jsonify, session, send_file
import os
import json
import asyncio
import threading
from datetime import datetime, timedelta
import pytz
import logging
from logging.handlers import RotatingFileHandler

# Custom utility imports
from utils.config import Config, Theme
from utils.encryption import save_credentials, load_credentials
from utils.screenshot import ScreenshotManager
from utils.scheduler import ScheduleManager
from utils.dashboard_manager import DashboardManager

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-change-this-in-production')

# Initialize managers
dashboard_manager = DashboardManager()
screenshot_manager = ScreenshotManager()
schedule_manager = ScheduleManager()

# Setup logging
os.makedirs(Config.LOG_DIR, exist_ok=True)
log_file = os.path.join(Config.LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    """Main application page with dashboard management interface"""
    # Load user preferences
    settings = load_user_settings()
    theme = settings.get('theme', 'light')
    
    # Get dashboard data
    dashboards = dashboard_manager.get_all_dashboards()
    lists = dashboard_manager.get_all_lists()
    schedules = schedule_manager.get_all_schedules()
    
    return render_template('index.html', 
                         dashboards=dashboards,
                         lists=lists,
                         schedules=schedules,
                         theme=theme)

@app.route('/api/credentials', methods=['GET', 'POST'])
def handle_credentials():
    """Handle credential management with secure encryption"""
    if request.method == 'GET':
        username, password = load_credentials()
        return jsonify({
            'has_credentials': username is not None and password is not None,
            'username': username if username else ''
        })
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'Username and password are required'}), 400
        
        success = save_credentials(username, password)
        if success:
            logger.info(f"Credentials saved for user: {username}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to save credentials'}), 500

@app.route('/api/dashboards', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handle_dashboards():
    """Handle dashboard CRUD operations"""
    if request.method == 'GET':
        dashboards = dashboard_manager.get_all_dashboards()
        return jsonify(dashboards)
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        dashboard = {
            'id': dashboard_manager.generate_id(),
            'name': data.get('name', '').strip(),
            'url': data.get('url', '').strip(),
            'lists': data.get('lists', []),
            'selected': False,
            'status': 'Ready',
            'created_at': datetime.now().isoformat()
        }
        
        # Validation
        if not dashboard['name']:
            return jsonify({'success': False, 'error': 'Dashboard name is required'}), 400
        if not dashboard['url']:
            return jsonify({'success': False, 'error': 'Dashboard URL is required'}), 400
        
        success = dashboard_manager.add_dashboard(dashboard)
        if success:
            logger.info(f"Added dashboard: {dashboard['name']}")
            return jsonify({'success': True, 'dashboard': dashboard})
        else:
            return jsonify({'success': False, 'error': 'Failed to add dashboard'}), 500
    
    elif request.method == 'PUT':
        data = request.get_json() or {}
        dashboard_id = data.get('id')
        if not dashboard_id:
            return jsonify({'success': False, 'error': 'Dashboard ID is required'}), 400
        
        success = dashboard_manager.update_dashboard(dashboard_id, data)
        if success:
            logger.info(f"Updated dashboard: {dashboard_id}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to update dashboard'}), 500
    
    elif request.method == 'DELETE':
        data = request.get_json() or {}
        dashboard_ids = data.get('ids', [])
        if not dashboard_ids:
            return jsonify({'success': False, 'error': 'No dashboards selected'}), 400
        
        success = dashboard_manager.delete_dashboards(dashboard_ids)
        if success:
            logger.info(f"Deleted dashboards: {dashboard_ids}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to delete dashboards'}), 500

@app.route('/api/lists', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handle_lists():
    """Handle dashboard list management"""
    if request.method == 'GET':
        lists = dashboard_manager.get_all_lists()
        return jsonify(lists)
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        list_name = data.get('name', '').strip()
        
        if not list_name:
            return jsonify({'success': False, 'error': 'List name is required'}), 400
        
        success = dashboard_manager.add_list(list_name)
        if success:
            logger.info(f"Added list: {list_name}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'List already exists'}), 400
    
    elif request.method == 'PUT':
        data = request.get_json() or {}
        old_name = data.get('old_name', '').strip()
        new_name = data.get('new_name', '').strip()
        
        if not old_name or not new_name:
            return jsonify({'success': False, 'error': 'Both old and new names are required'}), 400
        
        success = dashboard_manager.rename_list(old_name, new_name)
        if success:
            logger.info(f"Renamed list: {old_name} -> {new_name}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to rename list'}), 500
    
    elif request.method == 'DELETE':
        data = request.get_json() or {}
        list_name = data.get('name', '').strip()
        if not list_name:
            return jsonify({'success': False, 'error': 'List name is required'}), 400
        
        success = dashboard_manager.delete_list(list_name)
        if success:
            logger.info(f"Deleted list: {list_name}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to delete list'}), 500

@app.route('/api/screenshot', methods=['POST'])
def capture_screenshot():
    """Capture screenshots of selected dashboards"""
    data = request.get_json() or {}
    dashboard_ids = data.get('dashboard_ids', [])
    include_watermark = data.get('include_watermark', True)
    time_range = data.get('time_range', {})
    
    if not dashboard_ids:
        return jsonify({'success': False, 'error': 'No dashboards selected'}), 400
    
    # Get credentials
    username, password = load_credentials()
    if not username or not password:
        return jsonify({'success': False, 'error': 'Credentials not configured'}), 400
    
    # Start screenshot capture in background
    thread = threading.Thread(
        target=_capture_screenshots_async,
        args=(dashboard_ids, username, password, include_watermark, time_range)
    )
    thread.start()
    
    return jsonify({'success': True, 'message': 'Screenshot capture started'})

def _capture_screenshots_async(dashboard_ids, username, password, include_watermark, time_range):
    """Async function to capture screenshots"""
    try:
        # Check if there's already a running loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        dashboards = dashboard_manager.get_dashboards_by_ids(dashboard_ids)
        result = loop.run_until_complete(
            screenshot_manager.capture_screenshots(
                dashboards, username, password, include_watermark, time_range
            )
        )
        
        logger.info(f"Screenshot capture completed: {result}")
    except Exception as e:
        logger.error(f"Screenshot capture failed: {e}")
    finally:
        if 'loop' in locals() and not loop.is_running():
            loop.close()

@app.route('/api/schedules', methods=['GET', 'POST', 'PUT', 'DELETE'])
def handle_schedules():
    """Handle schedule management"""
    if request.method == 'GET':
        schedules = schedule_manager.get_all_schedules()
        return jsonify(schedules)
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        schedule = {
            'id': schedule_manager.generate_id(),
            'name': data.get('name', '').strip(),
            'dashboard_ids': data.get('dashboard_ids', []),
            'schedule_type': data.get('schedule_type', 'once'),
            'schedule_time': data.get('schedule_time', ''),
            'time_range': data.get('time_range', {}),
            'include_watermark': data.get('include_watermark', True),
            'active': True,
            'created_at': datetime.now().isoformat()
        }
        
        # Validation
        if not schedule['name']:
            return jsonify({'success': False, 'error': 'Schedule name is required'}), 400
        if not schedule['dashboard_ids']:
            return jsonify({'success': False, 'error': 'At least one dashboard must be selected'}), 400
        
        success = schedule_manager.add_schedule(schedule)
        if success:
            logger.info(f"Added schedule: {schedule['name']}")
            return jsonify({'success': True, 'schedule': schedule})
        else:
            return jsonify({'success': False, 'error': 'Failed to add schedule'}), 500
    
    elif request.method == 'PUT':
        data = request.get_json() or {}
        schedule_id = data.get('id')
        if not schedule_id:
            return jsonify({'success': False, 'error': 'Schedule ID is required'}), 400
        
        success = schedule_manager.update_schedule(schedule_id, data)
        if success:
            logger.info(f"Updated schedule: {schedule_id}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to update schedule'}), 500
    
    elif request.method == 'DELETE':
        data = request.get_json() or {}
        schedule_id = data.get('id')
        if not schedule_id:
            return jsonify({'success': False, 'error': 'Schedule ID is required'}), 400
        
        success = schedule_manager.delete_schedule(schedule_id)
        if success:
            logger.info(f"Deleted schedule: {schedule_id}")
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to delete schedule'}), 500

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    """Handle user settings (theme, preferences, etc.)"""
    if request.method == 'GET':
        settings = load_user_settings()
        return jsonify(settings)
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        success = save_user_settings(data)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to save settings'}), 500

def load_user_settings():
    """Load user settings from file"""
    try:
        if os.path.exists(Config.SETTINGS_FILE):
            with open(Config.SETTINGS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
    
    # Return default settings
    return {
        'theme': 'light',
        'auto_archive': True,
        'days_to_keep': 7,
        'timezone': 'America/New_York'
    }

def save_user_settings(settings):
    """Save user settings to file"""
    try:
        with open(Config.SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving settings: {e}")
        return False

if __name__ == '__main__':
    # Create necessary directories
    for directory in [Config.LOG_DIR, Config.TMP_DIR, Config.SCREENSHOT_ARCHIVE_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    # Start the schedule manager
    schedule_manager.start()
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)

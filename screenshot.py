"""
Screenshot Management Module
===========================

This module handles screenshot capture functionality using Playwright for browser automation.
It provides:
- Asynchronous screenshot capture with professional watermarks
- Support for multiple dashboard capture with concurrency control
- Smart authentication handling with bypass detection
- Enhanced watermark positioning and transparency
- Comprehensive error handling and logging
"""

import asyncio
import logging
import os
import io
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from PIL import Image, ImageDraw, ImageFont

from utils.config import Config, get_current_timestamp, sanitize_filename

logger = logging.getLogger(__name__)

class ScreenshotManager:
    """
    Manages screenshot capture operations for Splunk dashboards.
    
    This class handles:
    - Browser automation with Playwright
    - Concurrent screenshot capture with rate limiting
    - Professional watermark application
    - Authentication bypass detection
    - Error handling and retry logic
    """
    
    def __init__(self):
        """Initialize the screenshot manager."""
        self.max_concurrent = Config.MAX_CONCURRENT_SCREENSHOTS
        self.timeout = Config.SCREENSHOT_TIMEOUT * 1000  # Convert to milliseconds
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Ensure required directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories for screenshot storage."""
        for directory in [Config.TMP_DIR, Config.SCREENSHOT_ARCHIVE_DIR]:
            os.makedirs(directory, exist_ok=True)
        
        # Create today's directory
        today_str = datetime.now().strftime("%Y-%m-%d")
        today_dir = os.path.join(Config.TMP_DIR, today_str)
        os.makedirs(today_dir, exist_ok=True)
    
    async def capture_screenshots(self, dashboards: List[Dict], username: str, password: str, 
                                include_watermark: bool = True, time_range: Dict = None) -> Dict[str, Any]:
        """
        Capture screenshots for multiple dashboards concurrently.
        
        Args:
            dashboards (List[Dict]): List of dashboard configurations
            username (str): Splunk username for authentication
            password (str): Splunk password for authentication
            include_watermark (bool): Whether to add watermark to screenshots
            time_range (Dict): Time range configuration for dashboards
            
        Returns:
            Dict[str, Any]: Results of screenshot capture operation
        """
        logger.info(f"Starting screenshot capture for {len(dashboards)} dashboards")
        
        results = {
            'success': True,
            'total_dashboards': len(dashboards),
            'successful_captures': 0,
            'failed_captures': 0,
            'screenshots': [],
            'errors': []
        }
        
        # Prepare tasks for concurrent execution
        tasks = []
        for dashboard in dashboards:
            task = self._capture_single_dashboard(
                dashboard, username, password, include_watermark, time_range
            )
            tasks.append(task)
        
        # Execute all tasks concurrently with semaphore control
        screenshot_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(screenshot_results):
            dashboard = dashboards[i]
            
            if isinstance(result, Exception):
                # Task failed with exception
                error_msg = f"Dashboard '{dashboard['name']}': {str(result)}"
                results['errors'].append(error_msg)
                results['failed_captures'] += 1
                logger.error(error_msg)
            elif result['success']:
                # Task completed successfully
                results['screenshots'].append(result)
                results['successful_captures'] += 1
                logger.info(f"Successfully captured screenshot for '{dashboard['name']}'")
            else:
                # Task completed but failed
                error_msg = f"Dashboard '{dashboard['name']}': {result.get('error', 'Unknown error')}"
                results['errors'].append(error_msg)
                results['failed_captures'] += 1
                logger.error(error_msg)
        
        # Update overall success status
        results['success'] = results['failed_captures'] == 0
        
        logger.info(f"Screenshot capture completed. Success: {results['successful_captures']}, "
                   f"Failed: {results['failed_captures']}")
        
        return results
    
    async def _capture_single_dashboard(self, dashboard: Dict, username: str, password: str,
                                      include_watermark: bool, time_range: Dict = None) -> Dict[str, Any]:
        """
        Capture screenshot for a single dashboard.
        
        Args:
            dashboard (Dict): Dashboard configuration
            username (str): Splunk username
            password (str): Splunk password
            include_watermark (bool): Whether to add watermark
            time_range (Dict): Time range configuration
            
        Returns:
            Dict[str, Any]: Result of screenshot capture
        """
        async with self.semaphore:  # Limit concurrent operations
            try:
                async with async_playwright() as p:
                    # Launch browser in headless mode
                    browser = await p.chromium.launch(
                        headless=True,
                        args=['--no-sandbox', '--disable-dev-shm-usage']
                    )
                    
                    try:
                        # Create new page
                        page = await browser.new_page()
                        
                        # Set viewport for consistent screenshots
                        await page.set_viewport_size({'width': 1920, 'height': 1080})
                        
                        # Navigate to dashboard with time range
                        dashboard_url = self._build_dashboard_url(dashboard['url'], time_range)
                        logger.debug(f"Navigating to: {dashboard_url}")
                        
                        await page.goto(dashboard_url, wait_until='networkidle', timeout=self.timeout)
                        
                        # Handle authentication
                        auth_handled = await self._handle_authentication(page, username, password)
                        
                        if auth_handled:
                            # Wait for dashboard to load completely
                            await self._wait_for_dashboard_load(page)
                            
                            # Take screenshot
                            screenshot_bytes = await page.screenshot(
                                full_page=True,
                                type='png'
                            )
                            
                            # Process screenshot (add watermark if requested)
                            if include_watermark:
                                screenshot_bytes = self._add_watermark(screenshot_bytes, dashboard['name'])
                            
                            # Save screenshot
                            filename = self._generate_filename(dashboard['name'])
                            file_path = self._save_screenshot(screenshot_bytes, filename)
                            
                            return {
                                'success': True,
                                'dashboard_id': dashboard['id'],
                                'dashboard_name': dashboard['name'],
                                'file_path': file_path,
                                'file_size': len(screenshot_bytes),
                                'timestamp': get_current_timestamp()
                            }
                        else:
                            return {
                                'success': False,
                                'dashboard_id': dashboard['id'],
                                'dashboard_name': dashboard['name'],
                                'error': 'Authentication failed or login page not accessible'
                            }
                    
                    finally:
                        await browser.close()
            
            except PlaywrightTimeoutError:
                return {
                    'success': False,
                    'dashboard_id': dashboard['id'],
                    'dashboard_name': dashboard['name'],
                    'error': f'Timeout after {Config.SCREENSHOT_TIMEOUT} seconds'
                }
            except Exception as e:
                return {
                    'success': False,
                    'dashboard_id': dashboard['id'],
                    'dashboard_name': dashboard['name'],
                    'error': str(e)
                }
    
    def _build_dashboard_url(self, base_url: str, time_range: Dict = None) -> str:
        """
        Build dashboard URL with time range parameters.
        
        Args:
            base_url (str): Base dashboard URL
            time_range (Dict): Time range configuration
            
        Returns:
            str: Complete dashboard URL with parameters
        """
        if not time_range:
            return base_url
        
        # Parse the existing URL
        parsed = urlparse(base_url)
        query_params = parse_qs(parsed.query)
        
        # Add time range parameters based on type
        if time_range.get('type') == 'preset':
            preset = time_range.get('preset', 'last_24_hours')
            preset_config = self._get_time_range_preset(preset)
            query_params['earliest'] = [preset_config['earliest']]
            query_params['latest'] = [preset_config['latest']]
        elif time_range.get('type') == 'custom':
            if time_range.get('from') and time_range.get('to'):
                # Convert datetime-local format to Splunk format
                from_time = self._convert_datetime_to_splunk(time_range['from'])
                to_time = self._convert_datetime_to_splunk(time_range['to'])
                query_params['earliest'] = [from_time]
                query_params['latest'] = [to_time]
        
        # Rebuild URL with new query parameters
        new_query = urlencode(query_params, doseq=True)
        new_parsed = parsed._replace(query=new_query)
        
        return urlunparse(new_parsed)
    
    def _get_time_range_preset(self, preset: str) -> Dict[str, str]:
        """
        Get Splunk time range values for a preset.
        
        Args:
            preset (str): Preset name
            
        Returns:
            Dict[str, str]: Earliest and latest time values
        """
        presets = {
            'last_hour': {'earliest': '-1h@h', 'latest': 'now'},
            'last_4_hours': {'earliest': '-4h@h', 'latest': 'now'},
            'last_24_hours': {'earliest': '-24h@h', 'latest': 'now'},
            'last_7_days': {'earliest': '-7d@d', 'latest': 'now'},
            'last_30_days': {'earliest': '-30d@d', 'latest': 'now'},
            'today': {'earliest': '@d', 'latest': 'now'},
            'yesterday': {'earliest': '-1d@d', 'latest': '@d'},
            'this_week': {'earliest': '@w0', 'latest': 'now'},
            'last_week': {'earliest': '-1w@w0', 'latest': '@w0'},
            'this_month': {'earliest': '@mon', 'latest': 'now'},
            'last_month': {'earliest': '-1mon@mon', 'latest': '@mon'}
        }
        
        return presets.get(preset, presets['last_24_hours'])
    
    def _convert_datetime_to_splunk(self, datetime_str: str) -> str:
        """
        Convert HTML datetime-local format to Splunk time format.
        
        Args:
            datetime_str (str): Datetime in HTML format (YYYY-MM-DDTHH:MM)
            
        Returns:
            str: Splunk-compatible time string
        """
        try:
            # Parse HTML datetime format
            dt = datetime.fromisoformat(datetime_str)
            # Convert to Splunk format (MM/DD/YYYY:HH:MM:SS)
            return dt.strftime("%m/%d/%Y:%H:%M:%S")
        except Exception:
            # Fallback to current time
            return "now"
    
    async def _handle_authentication(self, page, username: str, password: str) -> bool:
        """
        Handle Splunk authentication with bypass detection.
        
        Args:
            page: Playwright page object
            username (str): Splunk username
            password (str): Splunk password
            
        Returns:
            bool: True if authentication successful or bypassed, False otherwise
        """
        try:
            # Wait a moment for page to fully load
            await asyncio.sleep(2)
            
            # Check if we're already on a dashboard (authentication bypassed)
            current_url = page.url
            
            # Look for dashboard indicators
            dashboard_indicators = [
                'app/search/dashboards',
                'app/splunk_monitoring_console',
                'app/',
                '/dashboard'
            ]
            
            is_dashboard = any(indicator in current_url.lower() for indicator in dashboard_indicators)
            
            # Check for dashboard-specific elements
            dashboard_elements = await page.query_selector_all(
                'div[data-view], .dashboard-view, .splunk-dashboard, .panel'
            )
            
            if is_dashboard or dashboard_elements:
                logger.info("Authentication bypassed - already on dashboard page")
                return True
            
            # Look for login form elements
            username_field = await page.query_selector('input[name="username"], input[id="username"], input[type="text"]')
            password_field = await page.query_selector('input[name="password"], input[id="password"], input[type="password"]')
            login_button = await page.query_selector('input[type="submit"], button[type="submit"], .loginButton, #loginButton')
            
            if username_field and password_field and login_button:
                logger.info("Login form detected, performing authentication")
                
                # Fill in credentials
                await username_field.fill(username)
                await password_field.fill(password)
                
                # Click login button
                await login_button.click()
                
                # Wait for navigation after login
                try:
                    await page.wait_for_load_state('networkidle', timeout=10000)
                    
                    # Check if login was successful by looking for dashboard elements
                    await asyncio.sleep(3)  # Give dashboard time to load
                    
                    # Verify we're not still on login page
                    current_url_after_login = page.url
                    if 'login' in current_url_after_login.lower():
                        logger.error("Still on login page after authentication attempt")
                        return False
                    
                    logger.info("Authentication successful")
                    return True
                    
                except PlaywrightTimeoutError:
                    logger.warning("Timeout waiting for page load after login")
                    return False
            else:
                # No login form found, assume we're already authenticated or it's not needed
                logger.info("No login form detected - assuming authentication not required")
                return True
        
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return False
    
    async def _wait_for_dashboard_load(self, page) -> None:
        """
        Wait for dashboard elements to fully load.
        
        Args:
            page: Playwright page object
        """
        try:
            # Wait for common dashboard elements
            await page.wait_for_selector(
                'div[data-view], .dashboard-view, .panel, .viz',
                timeout=15000
            )
            
            # Additional wait for dynamic content
            await asyncio.sleep(3)
            
            # Wait for any loading indicators to disappear
            loading_selectors = [
                '.loading', '.spinner', '.progress', '[data-loading="true"]'
            ]
            
            for selector in loading_selectors:
                try:
                    await page.wait_for_selector(selector, state='detached', timeout=5000)
                except PlaywrightTimeoutError:
                    pass  # Loading indicator might not exist
            
            logger.debug("Dashboard loading completed")
            
        except PlaywrightTimeoutError:
            logger.warning("Timeout waiting for dashboard elements to load")
        except Exception as e:
            logger.warning(f"Error waiting for dashboard load: {e}")
    
    def _add_watermark(self, screenshot_bytes: bytes, dashboard_name: str) -> bytes:
        """
        Add professional watermark to screenshot with enhanced positioning and transparency.
        
        Args:
            screenshot_bytes (bytes): Original screenshot data
            dashboard_name (str): Name of the dashboard for watermark
            
        Returns:
            bytes: Screenshot with watermark applied
        """
        try:
            # Open image from bytes
            image = Image.open(io.BytesIO(screenshot_bytes))
            
            # Create drawing context with RGBA mode for transparency
            draw = ImageDraw.Draw(image, "RGBA")
            
            # Generate watermark text
            timestamp = get_current_timestamp()
            watermark_text = f"Captured: {timestamp}\nDashboard: {dashboard_name}"
            
            # Try to load a professional font
            font_size = max(24, int(image.width * 0.015))  # Responsive font size
            try:
                # Try different font paths
                font_paths = [
                    "/System/Library/Fonts/Arial.ttf",  # macOS
                    "/usr/share/fonts/truetype/arial.ttf",  # Linux
                    "C:/Windows/Fonts/arial.ttf",  # Windows
                    "arial.ttf"  # Local
                ]
                
                font = None
                for font_path in font_paths:
                    try:
                        font = ImageFont.truetype(font_path, font_size)
                        break
                    except (OSError, IOError):
                        continue
                
                if not font:
                    # Fallback to default font
                    font = ImageFont.load_default()
                    
            except Exception:
                font = ImageFont.load_default()
            
            # Calculate text dimensions
            lines = watermark_text.split('\n')
            line_heights = []
            max_width = 0
            
            for line in lines:
                bbox = draw.textbbox((0, 0), line, font=font)
                line_width = bbox[2] - bbox[0]
                line_height = bbox[3] - bbox[1]
                line_heights.append(line_height)
                max_width = max(max_width, line_width)
            
            total_height = sum(line_heights) + (len(lines) - 1) * 5  # 5px line spacing
            
            # Position watermark in top-right corner with proper padding
            padding = 20
            x = image.width - max_width - padding
            y = padding
            
            # Draw semi-transparent background rectangle
            bg_padding = 12
            bg_rect = [
                x - bg_padding,
                y - bg_padding,
                x + max_width + bg_padding,
                y + total_height + bg_padding
            ]
            
            # Create gradient background for better visibility
            draw.rectangle(bg_rect, fill=(0, 0, 0, 180))  # Black with transparency
            
            # Add subtle border
            border_rect = [
                bg_rect[0] - 1,
                bg_rect[1] - 1,
                bg_rect[2] + 1,
                bg_rect[3] + 1
            ]
            draw.rectangle(border_rect, outline=(255, 255, 255, 100), width=1)
            
            # Draw text lines
            current_y = y
            for i, line in enumerate(lines):
                # Use different colors for different lines
                if i == 0:  # Timestamp line
                    text_color = (255, 255, 255, 255)  # White
                else:  # Dashboard name line
                    text_color = (200, 200, 255, 255)  # Light blue
                
                draw.text((x, current_y), line, fill=text_color, font=font)
                current_y += line_heights[i] + 5
            
            # Convert back to bytes
            output_buffer = io.BytesIO()
            image.convert("RGB").save(output_buffer, format="PNG", quality=95)
            
            logger.debug(f"Added professional watermark to screenshot for '{dashboard_name}'")
            return output_buffer.getvalue()
            
        except Exception as e:
            logger.error(f"Error adding watermark: {e}")
            # Return original screenshot if watermark fails
            return screenshot_bytes
    
    def _generate_filename(self, dashboard_name: str) -> str:
        """
        Generate a safe filename for the screenshot.
        
        Args:
            dashboard_name (str): Name of the dashboard
            
        Returns:
            str: Safe filename for the screenshot
        """
        # Sanitize dashboard name
        safe_name = sanitize_filename(dashboard_name)
        
        # Add timestamp for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Add unique suffix to prevent collisions
        unique_id = str(uuid.uuid4())[:8]
        
        return f"{safe_name}_{timestamp}_{unique_id}.png"
    
    def _save_screenshot(self, screenshot_bytes: bytes, filename: str) -> str:
        """
        Save screenshot to the temporary directory.
        
        Args:
            screenshot_bytes (bytes): Screenshot data
            filename (str): Filename for the screenshot
            
        Returns:
            str: Full path to the saved screenshot
        """
        # Create today's directory
        today_str = datetime.now().strftime("%Y-%m-%d")
        day_dir = os.path.join(Config.TMP_DIR, today_str)
        os.makedirs(day_dir, exist_ok=True)
        
        # Full file path
        file_path = os.path.join(day_dir, filename)
        
        # Save the file
        with open(file_path, 'wb') as f:
            f.write(screenshot_bytes)
        
        logger.debug(f"Saved screenshot to: {file_path}")
        return file_path
    
    def cleanup_old_screenshots(self) -> None:
        """
        Clean up old screenshot files based on retention policy.
        """
        try:
            now = datetime.now()
            
            # Clean up temp directory
            if os.path.exists(Config.TMP_DIR):
                for folder in os.listdir(Config.TMP_DIR):
                    folder_path = os.path.join(Config.TMP_DIR, folder)
                    if os.path.isdir(folder_path):
                        try:
                            folder_date = datetime.strptime(folder, "%Y-%m-%d")
                            if (now - folder_date).days > 1:  # Keep only today's screenshots in temp
                                # Move to archive instead of deleting
                                archive_path = os.path.join(Config.SCREENSHOT_ARCHIVE_DIR, folder)
                                if not os.path.exists(archive_path):
                                    os.rename(folder_path, archive_path)
                                    logger.info(f"Archived screenshot folder: {folder}")
                        except ValueError:
                            # Not a date folder, skip
                            continue
            
            # Clean up archive directory
            if os.path.exists(Config.SCREENSHOT_ARCHIVE_DIR):
                for folder in os.listdir(Config.SCREENSHOT_ARCHIVE_DIR):
                    folder_path = os.path.join(Config.SCREENSHOT_ARCHIVE_DIR, folder)
                    if os.path.isdir(folder_path):
                        try:
                            folder_date = datetime.strptime(folder, "%Y-%m-%d")
                            if (now - folder_date).days > Config.DAYS_TO_KEEP_ARCHIVES:
                                # Delete old archives
                                import shutil
                                shutil.rmtree(folder_path)
                                logger.info(f"Deleted old archive folder: {folder}")
                        except ValueError:
                            continue
            
        except Exception as e:
            logger.error(f"Error during screenshot cleanup: {e}")


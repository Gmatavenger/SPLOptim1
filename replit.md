# Splunk Dashboard Automator

## Overview

The Splunk Dashboard Automator is a modern web application built with Flask that provides automated screenshot capture and management of Splunk dashboards. The application offers a professional interface with light/dark theme support, secure credential management, advanced scheduling capabilities, and enhanced screenshot watermarking. Users can organize dashboards into lists, schedule automated captures, and manage their Splunk interactions through an intuitive web interface.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: Modern web interface using HTML5, CSS3, and vanilla JavaScript
- **UI Framework**: Bootstrap 5.3.0 for responsive design and components
- **Icon System**: Feather Icons for consistent iconography
- **Theme System**: CSS custom properties supporting light and dark themes with smooth transitions
- **Client-Side State Management**: JavaScript class-based architecture with real-time updates

### Backend Architecture
- **Framework**: Flask web framework with session management
- **Modular Design**: Utility modules separated by functionality (config, encryption, screenshot, scheduler, dashboard management)
- **Asynchronous Operations**: Asyncio integration for non-blocking screenshot capture
- **Background Processing**: Threading for scheduled tasks and long-running operations
- **Logging System**: Rotating file handlers with structured logging for troubleshooting

### Data Storage Solutions
- **Configuration Storage**: JSON files for dashboards, schedules, and settings
- **Credential Security**: Fernet symmetric encryption (AES 128 in CBC mode) for secure credential storage
- **File Organization**: Structured directory system with temporary files, logs, and screenshot archives
- **Data Validation**: URL validation, input sanitization, and data integrity checks

### Authentication and Authorization
- **Credential Encryption**: Cryptography library with Fernet for secure credential storage
- **Session Management**: Flask session handling with configurable secret keys
- **Authentication Bypass Detection**: Smart detection when Splunk login is not required
- **Secure Key Management**: Automatic encryption key generation and secure file permissions

### Browser Automation
- **Engine**: Playwright for reliable browser automation
- **Concurrency Control**: Semaphore-based rate limiting for multiple screenshot captures
- **Error Handling**: Comprehensive timeout and retry logic with detailed error reporting
- **Screenshot Enhancement**: PIL-based watermarking with professional timestamp overlays

### Scheduling System
- **Schedule Types**: Support for once, daily, weekly, and monthly recurring schedules
- **Background Execution**: Non-blocking schedule execution with status monitoring
- **Persistence**: JSON-based schedule storage with validation and conflict detection
- **Status Tracking**: Real-time schedule status updates with comprehensive logging

## External Dependencies

### Core Python Libraries
- **Flask**: Web framework for the main application server
- **Playwright**: Browser automation for screenshot capture
- **Cryptography**: Fernet encryption for secure credential storage
- **Pillow (PIL)**: Image processing for watermark application and screenshot enhancement
- **Pytz**: Timezone handling for accurate timestamp watermarks

### Frontend Dependencies
- **Bootstrap 5.3.0**: CSS framework delivered via CDN for responsive UI components
- **Feather Icons 4.28.0**: Icon library delivered via CDN for consistent iconography

### System Requirements
- **Python Environment**: Python 3.7+ with asyncio support
- **Browser Dependencies**: Playwright browser installations for Chrome/Chromium automation
- **File System**: Read/write access for configuration files, logs, and screenshot storage

### Optional Integrations
- **Splunk Enterprise/Cloud**: Target system for dashboard automation (requires valid credentials)
- **Network Access**: HTTPS connectivity to Splunk instances for authentication and screenshot capture
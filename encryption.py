"""
Encryption Utilities
===================

This module provides secure encryption and decryption functionality for
storing sensitive user credentials. It uses the Fernet symmetric encryption
algorithm from the cryptography library, which provides:
- AES 128 encryption in CBC mode
- HMAC using SHA256 for authentication
- Automatic key derivation and IV generation
"""

import os
import json
import logging
from typing import Tuple, Optional
from cryptography.fernet import Fernet
from utils.config import Config, SecurityConfig, get_current_timestamp

logger = logging.getLogger(__name__)

class CredentialManager:
    """
    Manages secure storage and retrieval of user credentials.
    
    This class handles:
    - Encryption key generation and storage
    - Secure credential encryption/decryption
    - File permission management for security
    - Error handling and logging
    """
    
    def __init__(self):
        """Initialize the credential manager."""
        self.key_file = Config.SECRETS_KEY_FILE
        self.credentials_file = Config.SECRETS_FILE
        self.fernet = None
        self._initialize_encryption()
    
    def _initialize_encryption(self):
        """
        Initialize the encryption system.
        Creates a new encryption key if one doesn't exist.
        """
        try:
            encryption_key = self._get_or_create_encryption_key()
            self.fernet = Fernet(encryption_key)
            logger.info("Encryption system initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize encryption: {e}")
            raise
    
    def _get_or_create_encryption_key(self) -> bytes:
        """
        Get existing encryption key or create a new one.
        
        Returns:
            bytes: Encryption key for Fernet
            
        Raises:
            Exception: If key generation or file operations fail
        """
        if os.path.exists(self.key_file):
            # Load existing key
            try:
                with open(self.key_file, "rb") as f:
                    key = f.read()
                logger.info("Loaded existing encryption key")
                return key
            except Exception as e:
                logger.error(f"Failed to load encryption key: {e}")
                raise
        else:
            # Generate new key
            try:
                key = Fernet.generate_key()
                with open(self.key_file, "wb") as f:
                    f.write(key)
                self._set_secure_permissions(self.key_file)
                logger.info("Generated new encryption key")
                return key
            except Exception as e:
                logger.error(f"Failed to generate encryption key: {e}")
                raise
    
    def _set_secure_permissions(self, file_path: str):
        """
        Set secure file permissions (Unix/Linux only).
        Makes the file readable/writable only by the owner.
        
        Args:
            file_path (str): Path to the file to secure
        """
        if os.name != 'nt':  # Not Windows
            try:
                os.chmod(file_path, SecurityConfig.SECURE_FILE_PERMISSIONS)
                logger.debug(f"Set secure permissions for {file_path}")
            except OSError as e:
                logger.warning(f"Could not set secure permissions for {file_path}: {e}")
    
    def save_credentials(self, username: str, password: str) -> bool:
        """
        Encrypt and save user credentials to file.
        
        Args:
            username (str): Splunk username
            password (str): Splunk password
            
        Returns:
            bool: True if credentials were saved successfully, False otherwise
        """
        if not self.fernet:
            logger.error("Encryption not initialized")
            return False
        
        if not username or not password:
            logger.error("Username and password cannot be empty")
            return False
        
        try:
            # Create credentials dictionary
            credentials = {
                "username": username.strip(),
                "password": password.strip(),
                "created_at": get_current_timestamp(),
                "version": "1.0"
            }
            
            # Convert to JSON and encrypt
            credentials_json = json.dumps(credentials).encode('utf-8')
            encrypted_data = self.fernet.encrypt(credentials_json)
            
            # Save to file
            with open(self.credentials_file, "wb") as f:
                f.write(encrypted_data)
            
            # Set secure permissions
            self._set_secure_permissions(self.credentials_file)
            
            logger.info(f"Credentials saved successfully for user: {username}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save credentials: {e}")
            return False
    
    def load_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Load and decrypt user credentials from file.
        
        Returns:
            Tuple[Optional[str], Optional[str]]: Username and password if successful,
                                               (None, None) if failed or not found
        """
        if not self.fernet:
            logger.error("Encryption not initialized")
            return None, None
        
        if not os.path.exists(self.credentials_file):
            logger.info("No credentials file found")
            return None, None
        
        try:
            # Read encrypted data
            with open(self.credentials_file, "rb") as f:
                encrypted_data = f.read()
            
            # Decrypt and parse
            decrypted_data = self.fernet.decrypt(encrypted_data)
            credentials = json.loads(decrypted_data.decode('utf-8'))
            
            username = credentials.get("username")
            password = credentials.get("password")
            
            if username and password:
                logger.info(f"Credentials loaded successfully for user: {username}")
                return username, password
            else:
                logger.warning("Credentials file contains invalid data")
                return None, None
                
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            # If decryption fails, the key might be corrupted or changed
            logger.warning("Consider deleting the credentials file and re-entering credentials")
            return None, None
    
    def delete_credentials(self) -> bool:
        """
        Delete stored credentials file.
        
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            if os.path.exists(self.credentials_file):
                os.remove(self.credentials_file)
                logger.info("Credentials file deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete credentials file: {e}")
            return False
    
    def credentials_exist(self) -> bool:
        """
        Check if credentials file exists.
        
        Returns:
            bool: True if credentials file exists, False otherwise
        """
        return os.path.exists(self.credentials_file)
    
    def validate_credentials_format(self, username: str, password: str) -> Tuple[bool, str]:
        """
        Validate credential format and requirements.
        
        Args:
            username (str): Username to validate
            password (str): Password to validate
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        if not username or not username.strip():
            return False, "Username cannot be empty"
        
        if not password or not password.strip():
            return False, "Password cannot be empty"
        
        if len(password) < SecurityConfig.MIN_PASSWORD_LENGTH:
            return False, f"Password must be at least {SecurityConfig.MIN_PASSWORD_LENGTH} characters"
        
        # Additional password requirements can be added here
        if SecurityConfig.REQUIRE_SPECIAL_CHARS:
            import re
            if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
                return False, "Password must contain at least one special character"
        
        if SecurityConfig.REQUIRE_NUMBERS:
            if not re.search(r'\d', password):
                return False, "Password must contain at least one number"
        
        if SecurityConfig.REQUIRE_UPPERCASE:
            if not re.search(r'[A-Z]', password):
                return False, "Password must contain at least one uppercase letter"
        
        return True, ""

# Global credential manager instance
_credential_manager = None

def get_credential_manager() -> CredentialManager:
    """
    Get the global credential manager instance.
    
    Returns:
        CredentialManager: Global credential manager instance
    """
    global _credential_manager
    if _credential_manager is None:
        _credential_manager = CredentialManager()
    return _credential_manager

def save_credentials(username: str, password: str) -> bool:
    """
    Convenience function to save credentials.
    
    Args:
        username (str): Splunk username
        password (str): Splunk password
        
    Returns:
        bool: True if successful, False otherwise
    """
    manager = get_credential_manager()
    return manager.save_credentials(username, password)

def load_credentials() -> Tuple[Optional[str], Optional[str]]:
    """
    Convenience function to load credentials.
    
    Returns:
        Tuple[Optional[str], Optional[str]]: Username and password if successful
    """
    manager = get_credential_manager()
    return manager.load_credentials()

def delete_credentials() -> bool:
    """
    Convenience function to delete credentials.
    
    Returns:
        bool: True if successful, False otherwise
    """
    manager = get_credential_manager()
    return manager.delete_credentials()

def credentials_exist() -> bool:
    """
    Convenience function to check if credentials exist.
    
    Returns:
        bool: True if credentials file exists, False otherwise
    """
    manager = get_credential_manager()
    return manager.credentials_exist()

def validate_credentials_format(username: str, password: str) -> Tuple[bool, str]:
    """
    Convenience function to validate credential format.
    
    Args:
        username (str): Username to validate
        password (str): Password to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    manager = get_credential_manager()
    return manager.validate_credentials_format(username, password)

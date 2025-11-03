"""
Output Manager Utility
Manages all output files (reports, logs, data) in the results folder
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

class OutputManager:
    """Manages output files in the results folder structure"""
    
    def __init__(self, base_dir: str = "results"):
        """
        Initialize output manager
        
        Args:
            base_dir: Base directory for all outputs (default: "results")
        """
        self.base_dir = Path(base_dir)
        self.ensure_directory_structure()
    
    def ensure_directory_structure(self):
        """Ensure the results directory structure exists"""
        directories = [
            self.base_dir,
            self.base_dir / "logs",
            self.base_dir / "reports",
            self.base_dir / "data",
            self.base_dir / "performance_data",
            self.base_dir / "test_configs",
            self.base_dir / "archive"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    def get_log_path(self, filename: str) -> str:
        """Get path for log files"""
        return str(self.base_dir / "logs" / filename)
    
    def get_report_path(self, filename: str) -> str:
        """Get path for report files"""
        return str(self.base_dir / "reports" / filename)
    
    def get_data_path(self, filename: str) -> str:
        """Get path for data files"""
        return str(self.base_dir / "data" / filename)
    
    def get_performance_path(self, filename: str) -> str:
        """Get path for performance data files"""
        return str(self.base_dir / "performance_data" / filename)
    
    def get_config_path(self, filename: str) -> str:
        """Get path for test config files"""
        return str(self.base_dir / "test_configs" / filename)
    
    def get_archive_path(self, filename: str) -> str:
        """Get path for archived files"""
        return str(self.base_dir / "archive" / filename)
    
    def get_timestamped_filename(self, base_name: str, extension: str = None) -> str:
        """
        Get a timestamped filename
        
        Args:
            base_name: Base name for the file
            extension: File extension (if None, will try to extract from base_name)
        
        Returns:
            Timestamped filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if extension is None:
            if '.' in base_name:
                name, ext = base_name.rsplit('.', 1)
                return f"{name}_{timestamp}.{ext}"
            else:
                return f"{base_name}_{timestamp}"
        else:
            return f"{base_name}_{timestamp}.{extension}"
    
    def get_tpcds_report_path(self, report_type: str = "tables") -> str:
        """Get path for TPC-DS specific reports"""
        filename = self.get_timestamped_filename(f"tpcds_{report_type}_report", "txt")
        return self.get_report_path(filename)
    
    def get_tpcds_data_path(self, report_type: str = "tables") -> str:
        """Get path for TPC-DS specific data files"""
        filename = self.get_timestamped_filename(f"tpcds_{report_type}_data", "csv")
        return self.get_data_path(filename)
    
    def get_logger_config(self, log_filename: str = "application.log") -> dict:
        """Get logging configuration for the results folder"""
        return {
            'level': logging.INFO,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'filename': self.get_log_path(log_filename)
        }

# Global instance for easy access
output_manager = OutputManager()


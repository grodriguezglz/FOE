"""
Database configuration for HEB scraper
"""
import os
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "heb_products"
    user: str = "memorodriguez"  # Your username
    password: str = ""
    min_connections: int = 1
    max_connections: int = 10

@dataclass
class ScraperConfig:
    """Scraper configuration"""
    batch_size: int = 1000
    max_retries: int = 3
    base_delay: int = 1
    max_pages_per_category: int = 100
    max_workers: int = 3  # Parallel processing

# Default configurations
DB_CONFIG = DatabaseConfig()
SCRAPER_CONFIG = ScraperConfig()
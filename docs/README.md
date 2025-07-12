# HEB Grocery Tracker

A comprehensive system for tracking HEB grocery prices and managing personal shopping data.

## Features

- **Price Scraping**: Automated collection of product prices from HEB website
- **PostgreSQL Database**: Robust data storage with proper relationships
- **Web Interface**: (Coming soon) Track weekly purchases and spending
- **Price Alerts**: (Coming soon) Get notified of price changes

## Quick Start

1. Install dependencies: `pip install -r scraper/requirements.txt`
2. Configure database: Update `scraper/config.py`
3. Run scraper: `python scraper/scraper.py`

## Database Schema

- **products**: Product information and categories
- **price_history**: Historical price data
- **users**: User accounts (for web app)
- **user_purchases**: Personal purchase tracking

## Project Structure

- `/scraper/`: Python scraping scripts
- `/database/`: SQL schemas and queries
- `/web-app/`: Web application (future)
- `/data/`: Data files and exports
- `/docs/`: Documentation
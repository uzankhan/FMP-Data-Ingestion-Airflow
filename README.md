# FMP Financial Data Pipeline

A production-grade data pipeline that fetches company financial data from the Financial Modeling Prep (FMP) API and processes it through Medallion Architecture using Apache Airflow.

## Project Overview

This pipeline fetches S&P 500 company symbols from Wikipedia, retrieves detailed company profiles from FMP API, and processes them through Bronze, Silver, and Gold layers.

## Architecture
[Wikipedia S&P 500] → [FMP API] → [Bronze: Raw JSON] → [Silver: Cleaned] → [Gold: Aggregated]

## Data Flow

### Step 1: Get S&P 500 Symbols
- Scrapes Wikipedia for S&P 500 constituent symbols
- Passes list to next task via XCom

### Step 2: Fetch Company Profiles
- Calls FMP API `/profile/{symbol}` endpoint for each symbol
- API limit: Free tier allows limited requests
- Returns JSON profiles

### Step 3: Bronze Layer (Raw Data)
- Saves raw JSON to S3 bucket
- Also loads into Snowflake `BRONZE.FMP_PROFILES`
- Preserves original data as immutable source

### Step 4: Silver Layer (Cleaned Data)
- Handles NULL values
- Standardizes country, industry, sector fields
- Calculates derived metrics:
  - Market Cap in Billions (B)
  - Revenue in Billions (B)
  - EBIT in Millions (M)
  - Net Income in Millions (M)
  - Profit Margin percentage

### Step 5: Gold Layer (Business Insights)
- Sector-wise summary (avg market cap, profit margin, total revenue)
- Top 10 companies by market cap

## Tech Stack

| Component | Technology |
| :--- | :--- |
| Orchestration | Apache Airflow |
| Cloud Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Data Source | FMP API |
| Scraping | BeautifulSoup |
| Containerization | Docker |

## Key Features

- ✅ XCom for inter-task communication
- ✅ Environment variables for security
- ✅ Medallion Architecture
- ✅ S3 and Snowflake integration
- ✅ Error handling and logging
- ✅ Email alerts on failure

## Snowflake Schema

### Bronze — `BRONZE.FMP_PROFILES`
- Raw API response with all fields
- `RAW_DATA` column stores full JSON

### Silver — `SILVER.COMPANY_PROFILES`
- Cleaned, standardized columns
- Calculated metrics (Market Cap B, Revenue B, etc.)

### Gold — `GOLD.SECTOR_SUMMARY`
- Sector-wise aggregations

### Gold — `GOLD.TOP_COMPANIES`
- Top 10 companies by market cap

## Business Value

- **Automation:** Fully automated daily pipeline
- **Data Quality:** Clean, standardized data
- **Scalability:** Can handle multiple APIs
- **Auditability:** Raw data preserved in Bronze
- **Insights:** Gold layer provides ready-to-use reports

## Author

Uzan Khan

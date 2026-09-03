from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 3),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['************']
}

def get_sp500_symbols(**context):
    """
    Task 1: Fetch S&P 500 symbols from Wikipedia
    """
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    try:
        response = requests.get(url, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        
        symbols = []
        for row in table.find_all('tr')[1:]:  # Skip header
            cols = row.find_all('td')
            if cols:
                ticker = cols[0].text.strip()
                symbols.append(ticker)
        
        logging.info(f"✅ Fetched {len(symbols)} S&P 500 symbols")
        
        # Limit for free tier
        symbols = symbols[:10]  # Limit to 10 for testing
        context['task_instance'].xcom_push(key='symbols', value=symbols)
        return symbols
        
    except Exception as e:
        logging.error(f"❌ Failed to fetch S&P 500: {e}")
        raise

def fetch_company_profiles(**context):
    """
    Task 2: Fetch company profiles from FMP API
    """
    ti = context['task_instance']
    symbols = ti.xcom_pull(task_ids='get_sp500_symbols', key='symbols')
    
    if not symbols:
        raise ValueError("No symbols received from XCom")
    
    api_key = Variable.get('fmp_api_key')
    base_url = 'https://financialmodelingprep.com/api/v3/profile/'
    
    profiles = []
    for symbol in symbols:
        try:
            url = f"{base_url}{symbol}?apikey={api_key}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data:
                profiles.append(data[0])
                logging.info(f"✅ Fetched profile for {symbol}")
        except Exception as e:
            logging.warning(f"⚠️ Failed to fetch {symbol}: {e}")
    
    logging.info(f"✅ Fetched {len(profiles)} company profiles")
    context['task_instance'].xcom_push(key='profiles', value=profiles)
    return profiles

def save_to_s3(**context):
    """
    Task 3: Save raw data to S3 (Bronze layer)
    """
    ti = context['task_instance']
    profiles = ti.xcom_pull(task_ids='fetch_company_profiles', key='profiles')
    
    if not profiles:
        raise ValueError("No profiles received from XCom")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    now = datetime.utcnow()
    s3_key = f"bronze/fmp/year={now.year}/month={now.month:02d}/day={now.day:02d}/{now.strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        s3_hook.load_string(
            string_data=json.dumps(profiles, indent=2),
            key=s3_key,
            bucket_name='fmp-raw-data-uzan',
            replace=True
        )
        logging.info(f"✅ Saved to S3: {s3_key}")
    except Exception as e:
        logging.error(f"❌ S3 upload failed: {e}")
        raise

def load_to_snowflake_bronze(**context):
    """
    Task 4: Load raw data to Snowflake Bronze table
    """
    ti = context['task_instance']
    profiles = ti.xcom_pull(task_ids='fetch_company_profiles', key='profiles')
    
    if not profiles:
        raise ValueError("No profiles received from XCom")
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    inserted_count = 0
    for profile in profiles:
        try:
            cursor.execute("""
                INSERT INTO BRONZE.FMP_PROFILES (
                    SYMBOL, COMPANY_NAME, INDUSTRY, SECTOR, COUNTRY,
                    CEO, WEBSITE, DESCRIPTION, MARKET_CAP, REVENUE,
                    EBIT, INCOME, PROFIT_MARGIN, RAW_DATA, INGESTION_TIME
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP()
                )
            """, (
                profile.get('symbol'),
                profile.get('companyName'),
                profile.get('industry'),
                profile.get('sector'),
                profile.get('country'),
                profile.get('ceo'),
                profile.get('website'),
                profile.get('description'),
                profile.get('mktCap'),
                profile.get('revenue'),
                profile.get('ebit'),
                profile.get('income'),
                profile.get('profitMargin'),
                json.dumps(profile)
            ))
            inserted_count += 1
        except Exception as e:
            logging.error(f"❌ Insert error for {profile.get('symbol')}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"✅ Loaded {inserted_count} profiles to Bronze")

def transform_to_silver(**context):
    """
    Task 5: Transform data to Silver layer
    - Clean NULLs
    - Standardize columns
    - Cast data types
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO SILVER.COMPANY_PROFILES (
                SYMBOL,
                COMPANY_NAME,
                INDUSTRY,
                SECTOR,
                COUNTRY,
                CEO,
                WEBSITE,
                DESCRIPTION,
                MARKET_CAP_B,
                REVENUE_B,
                EBIT_M,
                NET_INCOME_M,
                PROFIT_MARGIN,
                INGESTION_TIME
            )
            SELECT 
                SYMBOL,
                COMPANY_NAME,
                COALESCE(INDUSTRY, 'Unknown') as INDUSTRY,
                COALESCE(SECTOR, 'Unknown') as SECTOR,
                COALESCE(COUNTRY, 'Unknown') as COUNTRY,
                COALESCE(CEO, 'N/A') as CEO,
                WEBSITE,
                DESCRIPTION,
                ROUND(MARKET_CAP / 1000000000, 2) as MARKET_CAP_B,
                ROUND(REVENUE / 1000000000, 2) as REVENUE_B,
                ROUND(EBIT / 1000000, 2) as EBIT_M,
                ROUND(INCOME / 1000000, 2) as NET_INCOME_M,
                ROUND(PROFIT_MARGIN * 100, 2) as PROFIT_MARGIN,
                CURRENT_TIMESTAMP()
            FROM BRONZE.FMP_PROFILES
            WHERE INGESTION_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        """)
        conn.commit()
        logging.info("✅ Transformed data to Silver layer")
    except Exception as e:
        logging.error(f"❌ Silver transformation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def aggregate_to_gold(**context):
    """
    Task 6: Gold layer aggregations
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # 1. Sector-wise summary
        cursor.execute("""
            INSERT INTO GOLD.SECTOR_SUMMARY (
                SECTOR,
                TOTAL_COMPANIES,
                AVG_MARKET_CAP_B,
                AVG_PROFIT_MARGIN,
                TOTAL_REVENUE_B,
                UPDATED_AT
            )
            SELECT 
                SECTOR,
                COUNT(*) as TOTAL_COMPANIES,
                ROUND(AVG(MARKET_CAP_B), 2) as AVG_MARKET_CAP_B,
                ROUND(AVG(PROFIT_MARGIN), 2) as AVG_PROFIT_MARGIN,
                ROUND(SUM(REVENUE_B), 2) as TOTAL_REVENUE_B,
                CURRENT_TIMESTAMP()
            FROM SILVER.COMPANY_PROFILES
            GROUP BY SECTOR
        """)
        
        # 2. Top 10 companies by market cap
        cursor.execute("""
            INSERT INTO GOLD.TOP_COMPANIES (
                SYMBOL,
                COMPANY_NAME,
                SECTOR,
                MARKET_CAP_B,
                PROFIT_MARGIN,
                RANK,
                UPDATED_AT
            )
            SELECT 
                SYMBOL,
                COMPANY_NAME,
                SECTOR,
                MARKET_CAP_B,
                PROFIT_MARGIN,
                RANK() OVER (ORDER BY MARKET_CAP_B DESC) as RANK,
                CURRENT_TIMESTAMP()
            FROM SILVER.COMPANY_PROFILES
            QUALIFY RANK <= 10
        """)
        
        conn.commit()
        logging.info("✅ Gold aggregations complete!")
    except Exception as e:
        logging.error(f"❌ Gold aggregation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def send_notification(**context):
    """
    Task 7: Send completion notification
    """
    ti = context['task_instance']
    profiles = ti.xcom_pull(task_ids='fetch_company_profiles', key='profiles')
    count = len(profiles) if profiles else 0
    logging.info(f"✅ Pipeline completed successfully! Processed {count} companies")
    # Can add email/slack notification here

with DAG(
    'fmp_pipeline_dag',
    default_args=default_args,
    description='FMP Financial Data Pipeline with Bronze, Silver, Gold layers',
    schedule_interval='@daily',
    catchup=False,
    tags=['fmp', 'financial', 'etl']
) as dag:

    start = DummyOperator(task_id='start')

    get_symbols = PythonOperator(
        task_id='get_sp500_symbols',
        python_callable=get_sp500_symbols,
        provide_context=True
    )

    fetch_profiles = PythonOperator(
        task_id='fetch_company_profiles',
        python_callable=fetch_company_profiles,
        provide_context=True
    )

    save_to_s3_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3,
        provide_context=True
    )

    load_bronze = PythonOperator(
        task_id='load_to_snowflake_bronze',
        python_callable=load_to_snowflake_bronze,
        provide_context=True
    )

    transform_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
        provide_context=True
    )

    aggregate_gold = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
        provide_context=True
    )

    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    # Task Dependencies
    start >> get_symbols >> fetch_profiles >> save_to_s3_task >> load_bronze >> transform_silver >> aggregate_gold >> notify >> end

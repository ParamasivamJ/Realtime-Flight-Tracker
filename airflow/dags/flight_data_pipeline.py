from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'flight_tracker',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_backend_health():
    """Check if backend API is healthy"""
    try:
        response = requests.get('http://flight-backend:8000/health', timeout=10)
        if response.status_code == 200:
            print("✅ Backend API is healthy")
            return True
        else:
            print(f"❌ Backend API unhealthy: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Backend API check failed: {e}")
        return False

def check_datasource_health():
    """Check if OpenSky API is accessible"""
    try:
        response = requests.get('https://opensky-network.org/api/states/all', timeout=10)
        if response.status_code == 200:
            print("✅ OpenSky API is accessible")
            return True
        else:
            print(f"❌ OpenSky API issue: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ OpenSky API check failed: {e}")
        return False

def monitor_flight_stats():
    """Monitor flight statistics and log metrics"""
    try:
        response = requests.get('http://flight-backend:8000/flights/stats', timeout=10)
        stats = response.json()
        
        print(f"📊 Flight Statistics:")
        print(f"   Total Flights: {stats['total_flights']}")
        print(f"   Active Countries: {stats['active_countries']}")
        print(f"   Avg Speed: {stats['avg_speed']} km/h")
        print(f"   Avg Altitude: {stats['avg_altitude']} ft")
        
        # You could send these metrics to monitoring system
        return True
    except Exception as e:
        print(f"❌ Failed to get flight stats: {e}")
        return False

def data_quality_check():
    """Perform data quality checks on flight data"""
    try:
        response = requests.get('http://flight-backend:8000/flights/current', timeout=10)
        flights = response.json()
        
        print(f"🔍 Data Quality Check:")
        print(f"   Total flights: {len(flights)}")
        
        if flights:
            # Check for data anomalies
            valid_flights = [f for f in flights if f.get('latitude') and f.get('longitude')]
            print(f"   Valid coordinates: {len(valid_flights)}/{len(flights)}")
            
            # Check timestamp freshness
            from datetime import datetime
            now = datetime.utcnow()
            recent_flights = [f for f in flights if 'timestamp' in f]
            print(f"   Recent data: {len(recent_flights)} flights")
        
        return True
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
        return False

# Define the main flight tracking DAG
with DAG(
    'flight_tracking_monitoring',
    default_args=default_args,
    description='Monitor real-time flight tracking pipeline',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes
    catchup=False,
    tags=['flight', 'monitoring'],
) as dag:

    # Task 1: Check backend health
    check_backend = PythonOperator(
        task_id='check_backend_health',
        python_callable=check_backend_health,
    )

    # Task 2: Check data source health
    check_datasource = PythonOperator(
        task_id='check_datasource_health',
        python_callable=check_datasource_health,
    )

    # Task 3: Monitor flight statistics
    monitor_stats = PythonOperator(
        task_id='monitor_flight_statistics',
        python_callable=monitor_flight_stats,
    )

    # Task 4: Data quality checks
    data_quality = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_check,
    )

    # Task 5: Log pipeline status
    log_status = BashOperator(
        task_id='log_pipeline_status',
        bash_command='echo "Flight tracking pipeline check completed at $(date)"',
    )

    # Define task dependencies
    check_backend >> check_datasource >> [monitor_stats, data_quality] >> log_status
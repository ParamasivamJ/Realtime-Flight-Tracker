from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'flight_tracker',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_daily_report():
    """Generate daily flight statistics report"""
    try:
        # Get flight statistics
        response = requests.get('http://flight-backend:8000/flights/stats', timeout=10)
        stats = response.json()
        
        # Generate report (in real scenario, save to database/file)
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_flights': stats['total_flights'],
            'active_countries': stats['active_countries'],
            'avg_speed': stats['avg_speed'],
            'avg_altitude': stats['avg_altitude'],
            'report_type': 'daily_summary'
        }
        
        print(f"📈 Daily Report Generated:")
        print(f"   Flights: {report['total_flights']}")
        print(f"   Countries: {report['active_countries']}")
        print(f"   Avg Speed: {report['avg_speed']} km/h")
        
        # In production, you'd save this to Databricks or send as email
        return True
        
    except Exception as e:
        print(f"❌ Failed to generate daily report: {e}")
        return False

def cleanup_old_data():
    """Clean up old flight data (mock implementation)"""
    try:
        print("🧹 Cleaning up data older than 7 days...")
        # In production, this would run a Databricks cleanup job
        # Example: DELETE FROM flight_data WHERE timestamp < NOW() - INTERVAL 7 DAYS
        print("✅ Data cleanup completed")
        return True
    except Exception as e:
        print(f"❌ Data cleanup failed: {e}")
        return False

with DAG(
    'flight_data_processing',
    default_args=default_args,
    description='Batch processing for flight data',
    schedule_interval=timedelta(hours=24),  # Run daily
    catchup=False,
    tags=['flight', 'batch', 'processing'],
) as dag:

    daily_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
    )

    data_cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
    )

    daily_report >> data_cleanup
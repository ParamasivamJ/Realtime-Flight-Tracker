#!/usr/bin/env python3
"""
Script to verify Databricks integration
"""

import os
import requests
from datetime import datetime

def test_backend_connection():
    """Test backend API connection"""
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        print("✅ Backend API is running")
        print(f"   Response: {response.json()}")
        return True
    except Exception as e:
        print(f"❌ Backend API connection failed: {e}")
        return False

def test_databricks_connection():
    """Test Databricks connection through backend"""
    try:
        response = requests.get("http://localhost:8000/databricks/status", timeout=10)
        data = response.json()
        print(f"✅ Databricks status: {data['status']}")
        if data['status'] == 'connected':
            print("   🎉 Databricks is properly connected!")
        else:
            print(f"   ⚠️  {data.get('message', 'Unknown error')}")
        return data['status'] == 'connected'
    except Exception as e:
        print(f"❌ Databricks connection test failed: {e}")
        return False

def test_flight_data():
    """Test flight data endpoint"""
    try:
        response = requests.get("http://localhost:8000/flights/current", timeout=5)
        flights = response.json()
        print(f"✅ Flight data endpoint working - {len(flights)} flights")
        if flights:
            print(f"   Sample flight: {flights[0]['callsign']} from {flights[0]['origin_country']}")
        return True
    except Exception as e:
        print(f"❌ Flight data test failed: {e}")
        return False

def main():
    print("🚀 Testing Databricks Integration")
    print("=" * 50)
    
    # Check environment variables
    required_vars = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_HTTP_PATH']
    env_ok = True
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✅ {var} is set")
        else:
            print(f"❌ {var} is not set")
            env_ok = False
    
    print("\n" + "=" * 50)
    
    if env_ok:
        print("🎯 Testing connections...")
        backend_ok = test_backend_connection()
        databricks_ok = test_databricks_connection()
        flights_ok = test_flight_data()
        
        print("\n" + "=" * 50)
        if all([backend_ok, databricks_ok, flights_ok]):
            print("🎉 All tests passed! Databricks integration is working correctly.")
        else:
            print("⚠️  Some tests failed. Check your configuration.")
    else:
        print("❌ Please set all required environment variables in .env file")

if __name__ == "__main__":
    main()
import requests
import json

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Authenticate and get access token
def get_access_token():
    login_url = f"{SUPERSET_URL}/api/v1/security/login"
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }

    response = requests.post(login_url, json=payload)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"Login failed: {response.status_code} - {response.text}")
        return None

# Get CSRF token
def get_csrf_token(access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
    if response.status_code == 200:
        return response.json()["result"]
    else:
        print(f"Failed to get CSRF token: {response.status_code}")
        return None

# Create dataset
def create_dataset(access_token, csrf_token, table_name, schema="dev", database_id=1):
    url = f"{SUPERSET_URL}/api/v1/dataset/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }

    payload = {
        "database": database_id,
        "schema": schema,
        "table_name": table_name
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in [200, 201]:
        dataset_id = response.json()["id"]
        print(f"✓ Dataset created: {table_name} (ID: {dataset_id})")
        return dataset_id
    else:
        print(f"✗ Failed to create dataset {table_name}: {response.status_code} - {response.text}")
        return None

# Create chart
def create_chart(access_token, csrf_token, chart_config):
    url = f"{SUPERSET_URL}/api/v1/chart/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=chart_config)
    if response.status_code in [200, 201]:
        chart_id = response.json()["id"]
        print(f"✓ Chart created: {chart_config['slice_name']} (ID: {chart_id})")
        return chart_id
    else:
        print(f"✗ Failed to create chart {chart_config['slice_name']}: {response.status_code} - {response.text}")
        return None

def main():
    print("=" * 60)
    print("Superset Charts Creation Script")
    print("=" * 60)

    # Authenticate
    print("\n1. Authenticating...")
    access_token = get_access_token()
    if not access_token:
        print("Authentication failed. Exiting.")
        return
    print("✓ Authenticated successfully")

    # Get CSRF token
    print("\n2. Getting CSRF token...")
    csrf_token = get_csrf_token(access_token)
    if not csrf_token:
        print("Failed to get CSRF token. Exiting.")
        return
    print("✓ CSRF token obtained")

    # Create datasets for remaining mart tables
    print("\n3. Creating datasets...")
    datasets = {
        "mart_daily_summary": create_dataset(access_token, csrf_token, "mart_daily_summary"),
        "mart_air_quality": create_dataset(access_token, csrf_token, "mart_air_quality"),
        "mart_weather_trends": create_dataset(access_token, csrf_token, "mart_weather_trends")
    }

    # Filter out None values (failed creations)
    datasets = {k: v for k, v in datasets.items() if v is not None}

    if not datasets:
        print("No datasets created. Exiting.")
        return

    # Create charts
    print("\n4. Creating charts...")

    charts_config = []

    # Chart 1: Humidity Big Number (from mart_current_weather - dataset_id=1)
    charts_config.append({
        "slice_name": "Current Humidity",
        "viz_type": "big_number_total",
        "datasource_id": 1,
        "datasource_type": "table",
        "params": json.dumps({
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "humidity"},
                "aggregate": "SUM",
                "label": "SUM(humidity)"
            },
            "adhoc_filters": []
        })
    })

    # Chart 2: Wind Speed Big Number (from mart_current_weather)
    charts_config.append({
        "slice_name": "Current Wind Speed",
        "viz_type": "big_number_total",
        "datasource_id": 1,
        "datasource_type": "table",
        "params": json.dumps({
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "wind_speed"},
                "aggregate": "SUM",
                "label": "SUM(wind_speed)"
            },
            "adhoc_filters": []
        })
    })

    # Chart 3: Air Quality Index (from mart_air_quality)
    if "mart_air_quality" in datasets:
        charts_config.append({
            "slice_name": "Air Quality Index",
            "viz_type": "big_number_total",
            "datasource_id": datasets["mart_air_quality"],
            "datasource_type": "table",
            "params": json.dumps({
                "datasource": f"{datasets['mart_air_quality']}__table",
                "viz_type": "big_number_total",
                "metric": {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "latest_us_epa_index"},
                    "aggregate": "AVG",
                    "label": "AVG(latest_us_epa_index)"
                },
                "adhoc_filters": []
            })
        })

    # Chart 4: Temperature Range (from mart_daily_summary)
    if "mart_daily_summary" in datasets:
        charts_config.append({
            "slice_name": "Daily Temperature Range",
            "viz_type": "dist_bar",
            "datasource_id": datasets["mart_daily_summary"],
            "datasource_type": "table",
            "params": json.dumps({
                "datasource": f"{datasets['mart_daily_summary']}__table",
                "viz_type": "dist_bar",
                "metrics": [
                    {
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "avg_temperature"},
                        "aggregate": "AVG",
                        "label": "AVG(avg_temperature)"
                    }
                ],
                "groupby": ["weather_date"],
                "adhoc_filters": [],
                "row_limit": 10
            })
        })

    # Chart 5: PM2.5 Levels (from mart_air_quality)
    if "mart_air_quality" in datasets:
        charts_config.append({
            "slice_name": "PM2.5 Air Quality",
            "viz_type": "big_number_total",
            "datasource_id": datasets["mart_air_quality"],
            "datasource_type": "table",
            "params": json.dumps({
                "datasource": f"{datasets['mart_air_quality']}__table",
                "viz_type": "big_number_total",
                "metric": {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "latest_pm2_5"},
                    "aggregate": "AVG",
                    "label": "AVG(latest_pm2_5)"
                },
                "adhoc_filters": []
            })
        })

    # Create each chart
    created_charts = []
    for chart_config in charts_config:
        chart_id = create_chart(access_token, csrf_token, chart_config)
        if chart_id:
            created_charts.append(chart_id)

    print(f"\n{'=' * 60}")
    print(f"Summary:")
    print(f"  Datasets created: {len(datasets)}")
    print(f"  Charts created: {len(created_charts)}")
    print(f"{'=' * 60}")
    print("\n✓ All done! Visit http://localhost:8088/chart/list/ to view your charts")

if __name__ == "__main__":
    main()

import time
import json
import os
import urllib.request

class MetricsTracker:
    def __init__(self):
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.end_time = time.time()

    def get_duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0

def get_cluster_ram_used_mb(spark):
    try:
        app_id = spark.sparkContext.applicationId
        ui_url = spark.sparkContext.uiWebUrl
        api_url = f"{ui_url}/api/v1/applications/{app_id}/executors"

        req = urllib.request.Request(api_url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())

        # Суммируем поле 'memoryUsed' со всех воркеров
        total_used_bytes = sum(exec_data.get('memoryUsed', 0) for exec_data in data)

        # Переводим байты в мегабайты
        return total_used_bytes / (1024 * 1024)

    except Exception as e:
        print(f"Warning: Could not fetch cluster RAM from API: {e}")
        return 0.0


def save_report(experiment_name, duration, ram_mb, output_file="/report/metrics_report.json"):
    report = {
        "experiment": experiment_name,
        "duration_sec": round(duration, 2),
        "ram_cluster_mb": round(ram_mb, 2)
    }

    data = []
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                pass

    data.append(report)

    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

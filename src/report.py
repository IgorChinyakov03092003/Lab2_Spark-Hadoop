import time
import json
import os

class MetricsTracker:
    def __init__(self):
        self.stages = {}
        self.current_stage = None
        self.start_time = None
        self.total_start_time = None

    def start(self):
        self.total_start_time = time.time()

    def stop(self):
        self.total_duration = time.time() - self.total_start_time

    def start_stage(self, stage_name):
        self.current_stage = stage_name
        self.start_time = time.time()

    def end_stage(self):
        if self.current_stage and self.start_time:
            duration = time.time() - self.start_time
            self.stages[self.current_stage] = duration
            self.current_stage = None
            self.start_time = None

    def get_stages_duration(self):
        return self.stages

    def get_total_duration(self):
        return self.total_duration

def get_cluster_ram_used_mb(spark):
    try:
        memory_mx_bean = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
        return memory_mx_bean.getHeapMemoryUsage().getUsed() / (1024 * 1024)
    except:
        return 0

def save_report(experiment_name, total_duration, stages_dict, ram_mb, output_file="/report/metrics_report.json"):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    report = {
        "experiment": experiment_name,
        "total_duration_sec": round(total_duration, 2),
        "ram_cluster_mb": round(ram_mb, 2),
        "stages": {k: round(v, 2) for k, v in stages_dict.items()}
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

    print(f"Report saved to {output_file}")

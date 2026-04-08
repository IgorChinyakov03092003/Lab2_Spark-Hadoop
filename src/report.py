import time
import json
import os
import urllib.request

class MetricsTracker:
    def __init__(self, spark_context=None):
        self.stages = {}
        self.current_stage = None
        self.start_time = None
        self.total_start_time = None
        self.sc = spark_context

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

    def collect_api_stats(self, logger):
        """Сбор расширенной статистики по стадиям и джобам через Spark REST API"""
        stats = {
            "total_jobs": 0, "total_stages": 0, "completed_stages": 0,
            "shuffle_read_mb": 0.0, "shuffle_write_mb": 0.0
        }

        if not self.sc:
            return stats

        app_id = self.sc.applicationId
        base_api_url = f"http://localhost:4040/api/v1/applications/{app_id}"

        try:
            req = urllib.request.Request(f"{base_api_url}/stages")
            with urllib.request.urlopen(req) as response:
                stages_data = json.loads(response.read().decode('utf-8'))

            stats["total_stages"] = len(stages_data)

            logger.info(f"\n[Stage Overview] Total recorded: {len(stages_data)}")
            for stg in sorted(stages_data, key=lambda k: k.get('stageId', 0)):
                read_mb = stg.get('shuffleReadBytes', 0) / (1024 * 1024)
                write_mb = stg.get('shuffleWriteBytes', 0) / (1024 * 1024)

                stats["shuffle_read_mb"] += read_mb
                stats["shuffle_write_mb"] += write_mb

                if stg.get('status') == 'COMPLETE':
                    stats["completed_stages"] += 1
                    logger.info(f" -> [ID: {stg.get('stageId')}] {stg.get('name', 'N/A')} | Tasks: {stg.get('numCompleteTasks')} | R/W: {read_mb:.2f}MB / {write_mb:.2f}MB")

            req_jobs = urllib.request.Request(f"{base_api_url}/jobs")
            with urllib.request.urlopen(req_jobs) as response:
                jobs_data = json.loads(response.read().decode('utf-8'))

            stats["total_jobs"] = len(jobs_data)
            success_jobs = [j for j in jobs_data if j.get('status') == 'SUCCEEDED']

            logger.info(f"\n[Job Overview] Success: {len(success_jobs)} / Total: {len(jobs_data)}")
            for job in sorted(success_jobs, key=lambda k: k.get('jobId', 0)):
                logger.info(f" -> [Job {job.get('jobId')}] Linked stages: {job.get('stageIds', [])} | {job.get('name', 'N/A')}")

        except Exception as err:
            logger.warning(f"Could not fetch REST API metrics: {err}")

        return stats

def get_cluster_ram_used_mb(spark):
    try:
        memory_mx_bean = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
        return memory_mx_bean.getHeapMemoryUsage().getUsed() / (1024 * 1024)
    except:
        return 0

def save_report(exp_name, runtime, stage_times, cluster_ram, api_stats, output_file="/report/metrics_report.json"):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    payload = {
        "experiment": exp_name,
        "execution_time_sec": round(runtime, 2),
        "memory_used_mb": round(cluster_ram, 2),
        "spark_api_metrics": {
            "jobs_count": api_stats.get("total_jobs", 0),
            "stages_count": api_stats.get("total_stages", 0),
            "stages_completed": api_stats.get("completed_stages", 0),
            "shuffle_read_mb": round(api_stats.get("shuffle_read_mb", 0.0), 2),
            "shuffle_write_mb": round(api_stats.get("shuffle_write_mb", 0.0), 2)
        },
        "stages_timing": {k: round(v, 2) for k, v in stage_times.items()}
    }

    data = []
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                pass

    data.append(payload)

    with open(output_file, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Metrics successfully exported to {output_file}")

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col, countDistinct, rank, max, min
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from logger import get_logger
from report import MetricsTracker, save_report, get_cluster_ram_used_mb

def create_spark_session(is_optimized: bool) -> SparkSession:
    spark = SparkSession.builder \
        .appName(f"Lab2_Opt_{is_optimized}") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def load_and_clean_data(spark: SparkSession, path: str, multiply_factor: int):
    df_raw = spark.read.csv(path, header=True, inferSchema=True)
    df_clean = df_raw.dropna(subset=["product_id", "user_id", "category_id", "price"])

    multiplied_df = df_clean
    for _ in range(multiply_factor - 1):
        multiplied_df = multiplied_df.unionAll(df_clean)

    return multiplied_df

def apply_optimizations(df, spark: SparkSession, logger):
    total_cores = spark.sparkContext.defaultParallelism
    optimal_partitions = total_cores * 4  # Немного увеличим для тяжелого пайплайна

    logger.info(f"Applying optimization: repartition ({optimal_partitions} partitions) and cache...")
    df = df.repartition(optimal_partitions, "user_id")
    df.persist(StorageLevel.MEMORY_AND_DISK)

    # Активируем кэш (Action)
    cached_count = df.count()
    logger.info(f"Rows loaded to cache: {cached_count}")
    return df

def run_heavy_pipeline(df, tracker: MetricsTracker, logger):
    # Этап 1: Базовая агрегация по категориям
    tracker.start_stage("1_category_baseline")
    logger.info("Executing Stage 1: Category Baseline")
    category_baseline = df.groupBy("category_id") \
        .agg(
            avg("price").alias("avg_price"),
            max("price").alias("max_price"),
            min("price").alias("min_price"),
            count("product_id").alias("total_events")
        )
    # Форсируем выполнение (Action)
    category_baseline.count()
    tracker.end_stage()

    # Этап 2: Сегментация и профилирование юзеров
    tracker.start_stage("2_user_profiling")
    logger.info("Executing Stage 2: User Profiling")
    user_profiles = df.groupBy("user_id") \
        .agg(
            count("product_id").alias("user_events"),
            countDistinct("category_id").alias("unique_categories"),
            avg("price").alias("avg_check")
        )
    user_profiles.count()
    tracker.end_stage()

    # Этап 3: Поиск китов (Whales) и их любимых продуктов (Тяжелый Join)
    tracker.start_stage("3_whale_products")
    logger.info("Executing Stage 3: Whale Product Analysis")
    whales = user_profiles.filter(col("user_events") > 50).select("user_id")
    whale_activities = df.join(whales, on="user_id", how="inner")

    whale_products = whale_activities.groupBy("product_id", "category_id") \
        .agg(count("user_id").alias("whale_interactions"))
    whale_products.count()
    tracker.end_stage()

    # Этап 4: Ранжирование продуктов по категориям (Оконные функции - вызывают Shuffle)
    tracker.start_stage("4_window_ranking")
    logger.info("Executing Stage 4: Window Ranking Products")
    window_spec = Window.partitionBy("category_id").orderBy(desc("whale_interactions"))

    top_products_per_category = whale_products \
        .withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") <= 5)
    top_products_per_category.count()
    tracker.end_stage()

    # Этап 5: Финальный свод данных
    tracker.start_stage("5_final_aggregation")
    logger.info("Executing Stage 5: Final Aggregation")
    final_result = top_products_per_category.join(category_baseline, on="category_id", how="inner") \
        .orderBy(desc("total_events"), "category_id", "rank")

    final_result.show(10, truncate=False)
    tracker.end_stage()

def main():
    DATA_PATH = "hdfs://namenode:9000/user/data/2019-Dec.csv"

    parser = argparse.ArgumentParser(description="Spark Lab 2 Heavy Pipeline")
    parser.add_argument("--optimized", action="store_true", help="Run with optimizations")
    parser.add_argument("--nodes", type=int, default=1, help="Number of worker nodes")
    parser.add_argument("--multiply", type=int, default=1, help="Number of multiplication for current dataset")
    args = parser.parse_args()

    experiment_name = f"{args.nodes}Node_Opt-{args.optimized}"
    logger = get_logger(experiment_name)

    logger.info("Initializing application...")
    spark = create_spark_session(args.optimized)

    # Передаем контекст в трекер для доступа к API
    tracker = MetricsTracker(spark.sparkContext)
    tracker.start()

    tracker.start_stage("0_load_data")
    df_purchases = load_and_clean_data(spark, DATA_PATH, args.multiply)
    tracker.end_stage()

    if args.optimized:
        tracker.start_stage("0_optimization_overhead")
        df_optimized = apply_optimizations(df_purchases, spark, logger)
        tracker.end_stage()
    else:
        df_optimized = df_purchases

    run_heavy_pipeline(df_optimized, tracker, logger)

    tracker.stop()

    # Сбор базовой статистики
    total_time = tracker.get_total_duration()
    stage_timings = tracker.get_stages_duration()
    ram_usage = get_cluster_ram_used_mb(spark)

    # Сбор расширенной статистики через REST
    logger.info("Extracting advanced performance metrics...")
    api_metrics = tracker.collect_api_stats(logger)

    logger.info(f"Pipeline '{experiment_name}' completed in {total_time:.2f}s.")
    logger.info(f"Final Hardware & API Stats -> RAM Used: {ram_usage:.1f} MB | "
                f"Jobs: {api_metrics['total_jobs']} | "
                f"Stages Done: {api_metrics['completed_stages']}/{api_metrics['total_stages']} | "
                f"Shuffle: {api_metrics['shuffle_read_mb']:.1f}MB (In) / {api_metrics['shuffle_write_mb']:.1f}MB (Out)")

    save_report(
        exp_name=experiment_name,
        runtime=total_time,
        stage_times=stage_timings,
        cluster_ram=ram_usage,
        api_stats=api_metrics,
        output_file="/report/metrics_report.json"
    )

    logger.info("Sleeping for 1 minute to allow UI check...")
    time.sleep(60)
    spark.stop()

if __name__ == "__main__":
    main()

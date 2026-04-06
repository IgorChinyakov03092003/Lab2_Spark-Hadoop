import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col

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

    # Скейлим дату втупую, т.к. иначе выполняется слишком быстро
    multiplied_df = df_clean
    for _ in range(multiply_factor - 1):
        multiplied_df = multiplied_df.unionAll(df_clean)

    return multiplied_df


def apply_optimizations(df, is_optimized: bool, spark: SparkSession, logger):
    if is_optimized:
        # Динамически получаем количество доступных ядер в кластере
        total_cores = spark.sparkContext.defaultParallelism
        optimal_partitions = total_cores # * 3

        logger.info(f"Applying optimization: repartition ({optimal_partitions} partitions) and cache...")
        df = df.repartition(optimal_partitions, "user_id")
        df.cache()

        # Активируем кэш
        cached_count = df.count()
        logger.info(f"Rows loaded to cache: {cached_count}")
    return df


def execute_heavy_pipeline(df):
    # Находим среднюю цену товаров по каждой категории относительно всех кликов
    # Наша точка отчёта с обычными юзерами
    category_baseline = df.groupBy("category_id") \
        .agg(
            avg("price").alias("avg_category_price"),
            count("product_id").alias("total_events")
        )

    # Ищем самых активных юзеров (>50 действий)
    whales = df.groupBy("user_id") \
        .agg(count("product_id").alias("user_events")) \
        .filter(col("user_events") > 50) \
        .select("user_id")

    # Смотрим только действия самых активных юзеров (остальных отсекаем)
    whale_activities = df.join(whales, on="user_id", how="inner")

    # Какая самая популярная категория у наших частых клиентов
    whale_category_stats = whale_activities.groupBy("category_id") \
        .agg(count("product_id").alias("whale_events"))

    # Смотрим сортировку по категориям, и сортируем по тем, которые нравятся самым активным
    final_result = whale_category_stats.join(category_baseline, on="category_id", how="inner") \
        .orderBy(desc("whale_events"))

    return final_result


def run_pipeline(spark: SparkSession, data_path: str, is_optimized: bool, multiply_factor, logger):
    iter = 1

    logger.info(f"[{iter}]: Reading and preprocessing data from {data_path}")
    df_purchases = load_and_clean_data(spark, data_path, multiply_factor)
    iter += 1

    if is_optimized:
        logger.info(f"[{iter}]: Applying optimizations")
        df_optimized = apply_optimizations(df_purchases, is_optimized, spark, logger)
        iter += 1
    else:
        df_optimized = df_purchases

    logger.info(f"[{iter}]: Executing Heavy Pipeline (Whale Analysis)")
    result_df = execute_heavy_pipeline(df_optimized)
    iter += 1

    logger.info(f"[{iter}]: Execute action and show statistic")
    result_df.show(10, truncate=False)


def main():
    DATA_PATH = "hdfs://namenode:9000/user/data/2019-Dec.csv"

    # Инициализируем парсер, забираем число воркеров и оптимизацию
    parser = argparse.ArgumentParser(description="Spark Lab 2 Pipeline")
    parser.add_argument("--optimized", action="store_true", help="Run with optimizations")
    parser.add_argument("--nodes", type=int, default=1, help="Number of worker nodes")
    parser.add_argument("--multiply", type=int, default=1, help="Number of multiplication for current dataset (test scaling)")
    args = parser.parse_args()

    logger = get_logger()
    tracker = MetricsTracker()

    logger.info("Initializing application...")
    spark = create_spark_session(args.optimized)

    # Запускаем пайплайн и мерим производительность
    tracker.start()
    run_pipeline(spark, DATA_PATH, args.optimized, args.multiply, logger)
    tracker.stop()

    # Собираем статистику
    duration = tracker.get_duration()
    ram = get_cluster_ram_used_mb(spark)
    experiment_name = f"{args.nodes}Node_Opt-{args.optimized}"

    logger.info(f"Experiment {experiment_name} finished in {duration:.2f} seconds.")
    logger.info(f"Cluster RAM Used (Storage): {ram:.2f} MB")

    save_report(experiment_name, duration, ram, "/report/metrics_report.json")

    logger.info("Sleeping for 1 minute. Check Spark UI at http://localhost:4041")
    time.sleep(60)
    spark.stop()


if __name__ == "__main__":
    main()

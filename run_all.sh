#!/bin/bash
MULTIPLY_FACTOR=1

# Эксперимент 1
docker compose up -d
./setup_hdfs.sh
./run_experiment.sh --multiply $MULTIPLY_FACTOR
docker compose down -v

# Эксперимент 2
docker compose up -d
./setup_hdfs.sh
./run_experiment.sh --optimized --multiply $MULTIPLY_FACTOR
docker compose down -v

# Эксперимент 3
REPLICATION_FACTOR=3 docker compose up -d --scale datanode=3 --scale spark-worker=3
./setup_hdfs.sh
./run_experiment.sh --nodes 3 --multiply $MULTIPLY_FACTOR
docker compose down -v

# Эксперимент 4
REPLICATION_FACTOR=3 docker compose up -d --scale datanode=3 --scale spark-worker=3
./setup_hdfs.sh
./run_experiment.sh --nodes 3 --optimized --multiply $MULTIPLY_FACTOR
docker compose down -v

# Вывод графиков
python src/draw_graphics.py

#!/bin/bash
# Ждём, пока запустится NameNode
sleep 7
# Создаём папку
docker exec namenode hdfs dfs -mkdir -p /user/data
# Копируем файл с датой, разбиваем на блоки по 16 мегабайт
docker exec namenode hdfs dfs -D dfs.blocksize=16m -put /data/2019-Dec.csv /user/data/

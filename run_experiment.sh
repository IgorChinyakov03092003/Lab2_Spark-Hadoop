#!/bin/bash
chmod 777 ./report
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --py-files /src/logger.py,/src/report.py \
    /src/app.py "$@"

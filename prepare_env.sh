#!/bin/bash
# Качаем файл
FILENAME="data/2019-Dec.csv"
wget --no-check-certificate 'https://drive.usercontent.google.com/download?id=1Od5dMJsbs7N8bvLNZDSyNUOzCseBut2-&export=download&confirm=t' -O $FILENAME

# Ставим окружение
python -m venv .venv
source .venv/bin/activate
pip install matplotlib

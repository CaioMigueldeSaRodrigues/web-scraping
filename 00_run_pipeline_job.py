# Databricks notebook source

# COMMAND ----------

# Como o projeto está instalado como uma biblioteca (wheel),
# podemos importar 'src' diretamente de qualquer lugar.

from src.main import run_pipeline

run_pipeline() 
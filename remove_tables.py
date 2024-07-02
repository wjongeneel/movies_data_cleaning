# Databricks notebook source

tables = [
    'dbfs:/user/hive/warehouse/director',
    'dbfs:/user/hive/warehouse/genre',
    'dbfs:/user/hive/warehouse/star',
    'dbfs:/user/hive/warehouse/title',
    'dbfs:/user/hive/warehouse/movie_to_star',
    'dbfs:/user/hive/warehouse/movie',
    'dbfs:/user/hive/warehouse/movie_to_director',
    'dbfs:/user/hive/warehouse/movie_to_genre'
]
for table in tables: 
    print(table)
    dbutils.fs.rm(table, True)

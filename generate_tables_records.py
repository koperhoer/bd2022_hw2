# -*- coding: utf-8 -*-
## ^ Вы таки будете смеяться, но Питон отказывается запускаться
## из-под bash, если в скрипте есть комментарии на русском.
## Официальная нота протеста уже направлена в МИД США.
## А пока приходится решать проблему объявлением кодировки UTF-8.

## Будем пользоваться 64-битными целочисленными значениями из NumPy.
import numpy as np
## Необходимо парсить аргументы. В аргументы передаём объём
## генерируемых данных.
import sys
## Генерируем 64-битные инты питоновским рандомом.
import random

if len(sys.argv) != 2:
    print("Wrong number of arguments passed.")
    ##sys.exit(1)
data_volume = int(sys.argv[1])

## Сгенерим CQL-команды для исполнения кассандровским cqlsh.
f = open("create_tables.cql", "w")

## Если были такие таблицы и такое пространство ключей, какие мы
## хотим сделать, то предварительно их дропнуть.
## Создать пространство ключей с простой стратегией репликации
## с фактором 1, а также таблицу из чисел, факториал которых будем
## вычислять спарком. Пусть у каждого числа будет также
## свой интовый ключ.
f.write("DROP TABLE IF EXISTS hw2db.big_numbers1;\n\
DROP KEYSPACE IF EXISTS hw2db;\n\
CREATE KEYSPACE hw2db WITH replication = {\n\
    'class':'SimpleStrategy',\n\
    'replication_factor' : 1\n\
    };\n\
CREATE TABLE hw2db.big_numbers1 (\n\
    num_key int,\n\
    num_val bigint,\n\
    PRIMARY KEY (num_key));\n")
## Заполняем записи случайными числами от 0 до 127. 127! - это уже
## довольно немало. Пусть их в 100 раз больше, чем параметр.
for i in range(data_volume * 100):
    f.write("INSERT INTO hw2db.big_numbers1 (num_key, num_val)\n\
VALUES (" + str(i) + ", " + str(random.getrandbits(7)) + ");\n")

## Совершенно аналогично - следующие таблицы.
## Судя по заданию, на них необходимо сделать
## аналог wordcount для чисел и большого количества массивов.
## Допустим, что есть 100 таблиц с числами от 0 до 15,
## а вот сколько именно чисел, задаётся тем же параметром,
## что и количество факториалов.
for j in range(100):
    f.write("DROP TABLE IF EXISTS hw2db.num_arr" + str(j+1) + ";\n\
    CREATE TABLE hw2db.num_arr" + str(j+1) + " (\n\
        num_key int,\n\
        num_val bigint,\n\
        PRIMARY KEY (num_key));\n")

    for i in range(data_volume):
        f.write("INSERT INTO hw2db.num_arr" + str(j+1) + " (num_key, num_val)\n\
    VALUES (" + str(i) + ", " + str(random.getrandbits(4)) + ");\n")

## Не забыть закрыть файл.
f.close()
sys.exit(0)

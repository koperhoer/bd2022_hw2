: ' Хотим использовать красивые цвета.'
RED='\033[0;31m'
GREEN='\033[0;32m'
PURPLE='\033[0;35m'
NC='\033[0m'

: ' Измерим время выполнения всего скрипта.'
SECONDS=0

: ' Сюда записать путь до исполняемых файлов Apache Cassandra и Spark.'
CASSANDRA_PATH=/opt/apache-cassandra-4.0.3/bin/
SPARK_PATH=/opt/spark-3.2.1-bin-without-hadoop/bin/
: ' Путь до исполняемых файлов Hadoop.'
HADOOP_PATH=/opt/hadoop-2.10.1/bin/


: ' Может понадобиться обновить pip и поставить numpy.'
echo -e -n "Upgrading ${PURPLE}pip3${NC} if needed... "
python3 -m pip install --upgrade pip
: ' Проверяем на предмет кода ошибки после каждой команды.'
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} upgrading ${PURPLE}pip3${NC}."
       exit
fi
echo -e -n "Installing ${PURPLE}numpy${NC} if needed... "
python3 -m pip install numpy
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} installing ${PURPLE}numpy${NC}."
       exit
fi

: ' Запускаем питон, чтобы он выплюнул нам CQL-файл со структурой всех
    необходимых таблиц и с нагенерированными записями.
    Второй аргумент - объём данных (количество чисел и количество
    массивов соответственно).'
echo -e "Generating CQL file with ${PURPLE}Python${NC}."
python3 ./generate_tables_records.py $1
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} executing ${PURPLE}Python${NC}."
       exit
fi

: ' Остановим службы HDFS и YARN, если они были запущены.'
echo -e -n "Stopping existing ${PURPLE}HDFS${NC}... "
$HADOOP_PATH/../sbin/stop-dfs.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} stopping ${PURPLE}HDFS${NC}."
       exit
fi
echo -e -n "Stopping existing ${PURPLE}YARN${NC}... "
$HADOOP_PATH/../sbin/stop-yarn.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} stopping ${PURPLE}YARN${NC}."
       exit
fi

: ' Остановим кассандру, если она была запущена.'
echo -e "Killing existing ${PURPLE}Cassandra${NC}."
kill $(ps aux | grep '[c]assandra' | awk '{print $2}')
echo -e "${GREEN}All done${NC}."

: ' Запускаем кассандру. Если запускаем как root, кассандра требует флаг -R.'
if [ "$EUID" -ne 0 ]
  then echo -e "Launching ${PURPLE}Cassandra${NC} as non-root..."
       $CASSANDRA_PATH/cassandra
  else echo -e "Launching ${PURPLE}Cassandra${NC} as root..."
       $CASSANDRA_PATH/cassandra -R
fi
: 'К сожалению, ей нужно время развернуться, поэтому приходится ждать несколько секунд,
   прежде чем cqlsh сможет установить соединение с кассандрой.'
echo -e "Waiting for ${PURPLE}Cassandra${NC} to unfold..."
for i in {15..1}
do
   echo -e -n "$i..."
   sleep 1s
done
echo -e " Done."

: ' Запускаем кассандровскую cqlsh на создание таблиц и добавление записей.
    Все необходимые CQL-"запросы" - а значит, и структура таблиц - создаются питоном
    и записываются в CQL-файл.'
echo -e -n "Launching ${PURPLE}cqlsh${NC} on file create_tables.cql... "
$CASSANDRA_PATH/cqlsh --file=./create_tables.cql
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} launching ${PURPLE}Cassandra${NC} or executing ${PURPLE}cqlsh${NC}."
       exit
fi

: ' Результаты выгрузятся в HDFS. Запустим её, чтобы можно было
    сделать -get результатов, также запустим сам YARN.'
echo -e -n "Starting ${PURPLE}HDFS${NC} services... "
$HADOOP_PATH/../sbin/start-dfs.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} starting ${PURPLE}Hadoop DFS${NC}."
       exit
fi
echo -e -n "Starting ${PURPLE}YARN${NC}... "
$HADOOP_PATH/../sbin/start-yarn.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} starting ${PURPLE}YARN${NC}."
       exit
fi

: ' Безопасный режим HDFS мешает работе. Выходим из него.'
echo -e -n "Exiting ${PURPLE}HDFS${NC} NameNode safe mode... "
$HADOOP_PATH/hadoop dfsadmin -safemode leave
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} exiting ${PURPLE}HDFS${NC} NameNode safe mode."
       exit
fi

: ' Очистим выходную папку Spark, если она есть в HDFS.'
echo -e "Preparing ${PURPLE}HDFS${NC}... "
$HADOOP_PATH/hdfs dfs -rm -r hw2_spark_out

: ' Полагаем, что jar со всеми зависимостями у нас - первый аргумент.
    Тогда уже можно запускать Spark на взаимодействие с Cassandra.'
echo -e "Submitting ${PURPLE}Spark${NC} job."
$SPARK_PATH/spark-submit --class mephi.bd.SparkProgram --master local[*] --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 $2
: ' Выгрузим результаты из HDFS. Заменим ими существующие выходы, если они есть.'
echo -e "Attempting to get output from ${PURPLE}HDFS${NC}... "
rm -rf ./hw2_spark_out/
$HADOOP_PATH/hdfs dfs -get hw2_spark_out ./
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} running -get on ${PURPLE}HDFS${NC}."
       exit
fi

: ' Очистим выходную папку Spark.'
echo -e "Cleaning up ${PURPLE}HDFS${NC}... "
$HADOOP_PATH/hdfs dfs -rm -r hw2_spark_out
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} cleaning up ${PURPLE}HDFS${NC}."
       exit
fi

: ' Остановим службы HDFS и YARN.'
echo -e -n "Stopping ${PURPLE}HDFS${NC}... "
$HADOOP_PATH/../sbin/stop-dfs.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} stopping ${PURPLE}HDFS${NC}."
       exit
fi
echo -e -n "Stopping ${PURPLE}YARN${NC}... "
$HADOOP_PATH/../sbin/stop-yarn.sh
if [ $? -eq 0 ]
  then echo -e "${GREEN}Success${NC}."
  else echo -e "${RED}Failure${NC} stopping ${PURPLE}YARN${NC}."
       exit
fi

: ' Остановим кассандру.'
echo -e "Killing ${PURPLE}Cassandra${NC}."
kill $(ps aux | grep '[c]assandra' | awk '{print $2}')
echo -e "${GREEN}All done${NC}."
echo "Elapsed: $(($SECONDS / 3600))hrs $((($SECONDS / 60) % 60))min $(($SECONDS % 60))sec"
exit

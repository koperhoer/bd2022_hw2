package mephi.bd;

// Пользуемся пакетами Спарка, кортежом из Скалы и BigInteger для вычислений факториала.
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.math.BigInteger;

// Коннектор Спарка и Кассандры - от Datastax.
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

// Основной класс программы-драйвера.
public class SparkProgram {
    // Вычисление факториала на основе BigInteger.
    public static BigInteger factorial(BigInteger val)
    {
        BigInteger res = BigInteger.valueOf(1);
        if(val.equals(BigInteger.valueOf(1)) || val.equals(BigInteger.valueOf(0)))
            return BigInteger.valueOf(1);
        for (BigInteger i = BigInteger.valueOf(2); (i.compareTo(val) != 1); i = i.add(BigInteger.valueOf(1))) {
            res = res.multiply(i);
        }
        return res;
    }
    public static SparkConf conf;
    public static JavaSparkContext sc;
    public static JavaPairRDD<String, Tuple2<BigInteger, BigInteger>> ComputeIntensive(JavaRDD<BigInteger> bigNumbersRDD) {
        // Не будем выполнять лишние вычисления и по несколько раз вычислять факториал одного и того же числа.
        // Для этого сделаем distinct.
        JavaRDD<BigInteger> bigNumbersDistinctRDD = bigNumbersRDD.distinct();
        // Чтобы создать пары из значений числа до факториала и после факториала, приходится танцевать с бубном.
        // Создать копию RDD, отобразить каждый RDD в фиктивный "парный" RDD, и уже их джойнить воедино.
        // При этом один из RDD в трансформации map вычисляет факториал.
        JavaRDD<BigInteger> copyRDD = bigNumbersDistinctRDD;
        JavaPairRDD<String, BigInteger> leftPairRDD =
                bigNumbersDistinctRDD.mapToPair(x -> new Tuple2<>(x.toString(), x));
        JavaPairRDD<String, BigInteger> rightPairRDD =
                // Вычисление факториала - здесь.
                copyRDD.mapToPair(x -> new Tuple2<>(x.toString(), factorial(x)));
        // Теперь можно делать join и записывать результат в HDFS. Оттуда их достаёт bash.
        // Тут нет необходимости делать coalesce, и тем не менее, так удобнее читать результаты работы. Поэтому делаем coalesce.
        return leftPairRDD.join(rightPairRDD, 1).coalesce(1);
    }
    public static JavaPairRDD<BigInteger, Integer> NetworkIntensive(JavaRDD<BigInteger> arraysRDD) {
        // Возьмём само число как ключ, а значением зададим 1. Так можно будет сделать аналог wordcount на Spark с массивом чисел вместо текста.
        JavaPairRDD<BigInteger, Integer> arrayswkeysRDD =
                arraysRDD.mapToPair(x -> new Tuple2<>(x, Integer.valueOf(1)));
        // Тогда можно выполнить reduce сложением с неявной группировкой.
        // Снова делаем coalesce - только ради того, чтобы удобнее было руками разбираться в результатах.
        return arrayswkeysRDD.reduceByKey((x, y) -> (x + y)).coalesce(1);
    }
    public static void Initialise() {
        // Сконфигурировать спарк на общение с кассандрой.
        conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "127.0.0.1");
        // Задать название приложения.
        conf.setAppName("HW2: Spark program");
        // Настроить выполнение в локальном режиме.
        conf.setMaster("local[1]");
        // Создаём контекст спарка.
        sc = new JavaSparkContext(conf);
    }
    // Основной метод драйвера.
    public static void main(String[] args) throws Exception {
        // Инициализировать конфигурацию и контекст.
        Initialise();
        // Подтягиваем данные из кассандры. Чтобы вычислить факториал большого числа, нам его ключ в БД не нужен.
        // Соответственно, достаточно взять только само значение числа.
        JavaRDD<BigInteger> bigNumbersRDD = javaFunctions(sc)
                .cassandraTable("hw2db", "big_numbers1", mapColumnTo(BigInteger.class))
                .select("num_val");
        // Выполняем "сложную" по вычислениям задачу (см. соответствующий метод).
        // Запись результата в HDFS для последующего выполнения get башом.
        ComputeIntensive(bigNumbersRDD).saveAsTextFile("/user/root/hw2_spark_out/comp_intensive");
        // Для тяжёлых операций с данными рассмотрим массив таблиц того же вида из БД.
        for(Integer i = 1; i <= Integer.valueOf(100); i++) {
            JavaRDD<BigInteger> arraysRDD = javaFunctions(sc)
                    .cassandraTable("hw2db", "num_arr" + i.toString(), mapColumnTo(BigInteger.class))
                    .select("num_val");
            // Результат записать в HDFS, выгрузка - башом.
            NetworkIntensive(arraysRDD).saveAsTextFile("/user/root/hw2_spark_out/data_intensive" + i.toString());
        }
    }
}
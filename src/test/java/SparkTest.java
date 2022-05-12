// Используем наш пакет, junit и BigInteger.
import mephi.bd.SparkProgram;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

public class SparkTest {
    /*
    @Test
    // Тест "вычислительно сложной" задачи.
    public void testComputeIntensive() {
        // На её вход должен поступить RDD из BigInteger.
        SparkProgram sp = new SparkProgram();
        sp.Initialise();
        // Пусть SparkProgram возьмёт факториалы чисел 4, 3 и 2.
        List<BigInteger> bigIntegers = Arrays.asList(BigInteger.valueOf(4), BigInteger.valueOf(3), BigInteger.valueOf(2));
        // Здесь сделаем все те же операции и сравним результаты.
        JavaRDD<BigInteger> input = sp.sc.parallelize(bigIntegers);
        JavaRDD<BigInteger> input2 = sp.sc.parallelize(bigIntegers);
        JavaPairRDD<String, BigInteger> leftPairRDD =
                input.mapToPair(x -> new Tuple2<>(x.toString(), x));
        JavaPairRDD<String, BigInteger> rightPairRDD =
                input2.mapToPair(x -> new Tuple2<>(x.toString(), SparkProgram.factorial(x)));
        System.out.println(leftPairRDD.join(rightPairRDD, 1).coalesce(1) == sp.ComputeIntensive(input));
    }
    @Test
    // Тест нагружающей сеть задачи.
    public void testNetworkIntensive() {
        // На её вход должен поступить RDD из BigInteger.
        SparkProgram sp = new SparkProgram();
        sp.Initialise();
        // Пусть SparkProgram сделает wordcount на числах 4, 4 и 2.
        List<BigInteger> arrays = Arrays.asList(BigInteger.valueOf(4), BigInteger.valueOf(4), BigInteger.valueOf(2));
        JavaRDD<BigInteger> input = sp.sc.parallelize(arrays);
        // Здесь сделаем все те же операции и сравним результаты.
        JavaPairRDD<BigInteger, Integer> arrayswkeysRDD =
                input.mapToPair(x -> new Tuple2<>(x, Integer.valueOf(1)));
        System.out.println(arrayswkeysRDD.reduceByKey((x, y) -> (x + y)).coalesce(1) == sp.NetworkIntensive(input));
    }
    */
}

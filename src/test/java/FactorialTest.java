// Используем наш пакет, junit и BigInteger.
import mephi.bd.SparkProgram;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class FactorialTest {
    SparkProgram sp;

    @Before
    // Инстанциируем экземпляр программы.
    public void setUp() {
        sp = new SparkProgram();
    }

    @Test
    public void testUpto10() {
        // Прогоним метод, берущий факториал, на числах от 1 до 10. Больше и не надо.
        assertEquals("Expected 1", BigInteger.valueOf(1), sp.factorial(BigInteger.valueOf(0)));
        assertEquals("Expected 1", BigInteger.valueOf(1), sp.factorial(BigInteger.valueOf(1)));
        assertEquals("Expected 2", BigInteger.valueOf(2), sp.factorial(BigInteger.valueOf(2)));
        assertEquals("Expected 6", BigInteger.valueOf(6), sp.factorial(BigInteger.valueOf(3)));
        assertEquals("Expected 24", BigInteger.valueOf(24), sp.factorial(BigInteger.valueOf(4)));
        assertEquals("Expected 120", BigInteger.valueOf(120), sp.factorial(BigInteger.valueOf(5)));
        assertEquals("Expected 720", BigInteger.valueOf(720), sp.factorial(BigInteger.valueOf(6)));
        assertEquals("Expected 5040", BigInteger.valueOf(5040), sp.factorial(BigInteger.valueOf(7)));
        assertEquals("Expected 40320", BigInteger.valueOf(40320), sp.factorial(BigInteger.valueOf(8)));
        assertEquals("Expected 362880", BigInteger.valueOf(362880), sp.factorial(BigInteger.valueOf(9)));
        assertEquals("Expected 3628800", BigInteger.valueOf(3628800), sp.factorial(BigInteger.valueOf(10)));
    }

}

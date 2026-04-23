package org.howard.edu.lsp.finalexam.question3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 test class for GradeCalculator.
 */
public class GradeCalculatorTest {

    private GradeCalculator calculator;

    @BeforeEach
    public void setUp() {
        calculator = new GradeCalculator();
    }

    // -----------------------------------------------------------------------
    // average()
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Test average() returns correct result for typical scores")
    public void testAverageTypical() {
        double result = calculator.average(90, 80, 70);
        assertEquals(80.0, result, 0.001);
    }

    // -----------------------------------------------------------------------
    // letterGrade()
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Test letterGrade() returns correct letter for a B average")
    public void testLetterGradeB() {
        assertEquals("B", calculator.letterGrade(85.0));
    }

    @Test
    @DisplayName("Test letterGrade() returns F for average below 60")
    public void testLetterGradeF() {
        assertEquals("F", calculator.letterGrade(55.0));
    }

    // -----------------------------------------------------------------------
    // isPassing()
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Test isPassing() returns true for passing average")
    public void testIsPassingTrue() {
        assertTrue(calculator.isPassing(75.0));
    }

    @Test
    @DisplayName("Test isPassing() returns false for failing average")
    public void testIsPassingFalse() {
        assertFalse(calculator.isPassing(59.9));
    }

    // -----------------------------------------------------------------------
    // Boundary-value tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Boundary: average of exactly 60 should return D and be passing")
    public void testBoundaryExactly60() {
        double avg = calculator.average(60, 60, 60);
        assertEquals(60.0, avg, 0.001);
        assertEquals("D", calculator.letterGrade(avg));
        assertTrue(calculator.isPassing(avg));
    }

    @Test
    @DisplayName("Boundary: average of exactly 90 should return A")
    public void testBoundaryExactly90() {
        double avg = calculator.average(90, 90, 90);
        assertEquals(90.0, avg, 0.001);
        assertEquals("A", calculator.letterGrade(avg));
    }

    // -----------------------------------------------------------------------
    // Exception tests
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Exception: average() throws IllegalArgumentException for score above 100")
    public void testAverageScoreAbove100() {
        assertThrows(IllegalArgumentException.class, () -> {
            calculator.average(101, 80, 70);
        });
    }

    @Test
    @DisplayName("Exception: average() throws IllegalArgumentException for negative score")
    public void testAverageNegativeScore() {
        assertThrows(IllegalArgumentException.class, () -> {
            calculator.average(90, -1, 70);
        });
    }
}

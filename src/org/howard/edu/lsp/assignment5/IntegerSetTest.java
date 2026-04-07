package org.howard.edu.lsp.assignment5;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 test class for IntegerSet.
 */
public class IntegerSetTest {

    private IntegerSet set1;
    private IntegerSet set2;

    @BeforeEach
    public void setUp() {
        set1 = new IntegerSet();
        set2 = new IntegerSet();
    }

    // -------------------------------------------------------------------------
    // clear()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test clear() empties a non-empty set")
    public void testClear() {
        set1.add(1);
        set1.add(2);
        set1.clear();
        assertTrue(set1.isEmpty());
        assertEquals(0, set1.length());
    }

    @Test
    @DisplayName("Test clear() on an already-empty set has no effect")
    public void testClearEmpty() {
        set1.clear();
        assertTrue(set1.isEmpty());
    }

    // -------------------------------------------------------------------------
    // length()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test length() on empty set returns 0")
    public void testLengthEmpty() {
        assertEquals(0, set1.length());
    }

    @Test
    @DisplayName("Test length() returns correct count")
    public void testLength() {
        set1.add(10);
        set1.add(20);
        set1.add(30);
        assertEquals(3, set1.length());
    }

    // -------------------------------------------------------------------------
    // isEmpty()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test isEmpty() returns true on empty set")
    public void testIsEmptyTrue() {
        assertTrue(set1.isEmpty());
    }

    @Test
    @DisplayName("Test isEmpty() returns false on non-empty set")
    public void testIsEmptyFalse() {
        set1.add(5);
        assertFalse(set1.isEmpty());
    }

    // -------------------------------------------------------------------------
    // contains()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test contains() returns true for present value")
    public void testContainsPresent() {
        set1.add(7);
        assertTrue(set1.contains(7));
    }

    @Test
    @DisplayName("Test contains() returns false for absent value")
    public void testContainsAbsent() {
        set1.add(7);
        assertFalse(set1.contains(99));
    }

    @Test
    @DisplayName("Test contains() on empty set returns false")
    public void testContainsEmpty() {
        assertFalse(set1.contains(1));
    }

    // -------------------------------------------------------------------------
    // add()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test add() inserts a new element")
    public void testAdd() {
        set1.add(3);
        assertTrue(set1.contains(3));
        assertEquals(1, set1.length());
    }

    @Test
    @DisplayName("Test add() does not insert duplicates")
    public void testAddDuplicate() {
        set1.add(3);
        set1.add(3);
        assertEquals(1, set1.length());
    }

    // -------------------------------------------------------------------------
    // remove()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test remove() removes a present element")
    public void testRemovePresent() {
        set1.add(1);
        set1.add(2);
        set1.remove(1);
        assertFalse(set1.contains(1));
        assertEquals(1, set1.length());
    }

    @Test
    @DisplayName("Test remove() is a no-op for absent element")
    public void testRemoveAbsent() {
        set1.add(1);
        set1.remove(99);
        assertEquals(1, set1.length());
        assertTrue(set1.contains(1));
    }

    // -------------------------------------------------------------------------
    // largest()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test largest() returns maximum value")
    public void testLargest() {
        set1.add(3);
        set1.add(1);
        set1.add(7);
        assertEquals(7, set1.largest());
    }

    @Test
    @DisplayName("Test largest() throws IntegerSetException on empty set")
    public void testLargestEmpty() {
        assertThrows(IntegerSetException.class, () -> set1.largest());
    }

    // -------------------------------------------------------------------------
    // smallest()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test smallest() returns minimum value")
    public void testSmallest() {
        set1.add(3);
        set1.add(1);
        set1.add(7);
        assertEquals(1, set1.smallest());
    }

    @Test
    @DisplayName("Test smallest() throws IntegerSetException on empty set")
    public void testSmallestEmpty() {
        assertThrows(IntegerSetException.class, () -> set1.smallest());
    }

    // -------------------------------------------------------------------------
    // equals()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test equals() returns true for same elements in different order")
    public void testEqualsTrue() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(3); set2.add(1); set2.add(2);
        assertTrue(set1.equals(set2));
    }

    @Test
    @DisplayName("Test equals() returns false when sets differ")
    public void testEqualsFalse() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(1); set2.add(2);
        assertFalse(set1.equals(set2));
    }

    @Test
    @DisplayName("Test equals() returns true for two empty sets")
    public void testEqualsBothEmpty() {
        assertTrue(set1.equals(set2));
    }

    // -------------------------------------------------------------------------
    // union()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test union() contains all elements from both sets")
    public void testUnion() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        IntegerSet result = set1.union(set2);
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
        assertTrue(result.contains(4));
        assertEquals(4, result.length());
    }

    @Test
    @DisplayName("Test union() does not modify original sets")
    public void testUnionOriginalUnchanged() {
        set1.add(1); set1.add(2);
        set2.add(3); set2.add(4);
        set1.union(set2);
        assertEquals(2, set1.length());
        assertEquals(2, set2.length());
    }

    @Test
    @DisplayName("Test union() with empty set returns copy of the other")
    public void testUnionWithEmpty() {
        set1.add(1); set1.add(2);
        IntegerSet result = set1.union(set2);
        assertEquals(2, result.length());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
    }

    // -------------------------------------------------------------------------
    // intersect()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test intersect() returns only common elements")
    public void testIntersect() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        IntegerSet result = set1.intersect(set2);
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
        assertFalse(result.contains(1));
        assertFalse(result.contains(4));
        assertEquals(2, result.length());
    }

    @Test
    @DisplayName("Test intersect() does not modify original sets")
    public void testIntersectOriginalUnchanged() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        set1.intersect(set2);
        assertEquals(3, set1.length());
        assertEquals(3, set2.length());
    }

    @Test
    @DisplayName("Test intersect() with disjoint sets returns empty set")
    public void testIntersectDisjoint() {
        set1.add(1); set1.add(2);
        set2.add(3); set2.add(4);
        IntegerSet result = set1.intersect(set2);
        assertTrue(result.isEmpty());
    }

    // -------------------------------------------------------------------------
    // diff()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test diff() returns elements in set1 not in set2")
    public void testDiff() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        IntegerSet result = set1.diff(set2);
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
        assertFalse(result.contains(3));
        assertEquals(1, result.length());
    }

    @Test
    @DisplayName("Test diff() does not modify original sets")
    public void testDiffOriginalUnchanged() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        set1.diff(set2);
        assertEquals(3, set1.length());
        assertEquals(3, set2.length());
    }

    @Test
    @DisplayName("Test diff() with empty second set returns copy of first")
    public void testDiffEmptySecond() {
        set1.add(1); set1.add(2);
        IntegerSet result = set1.diff(set2);
        assertEquals(2, result.length());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
    }

    // -------------------------------------------------------------------------
    // complement()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test complement() returns elements in set2 not in set1")
    public void testComplement() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        IntegerSet result = set1.complement(set2);
        assertTrue(result.contains(4));
        assertFalse(result.contains(2));
        assertFalse(result.contains(3));
        assertEquals(1, result.length());
    }

    @Test
    @DisplayName("Test complement() does not modify original sets")
    public void testComplementOriginalUnchanged() {
        set1.add(1); set1.add(2); set1.add(3);
        set2.add(2); set2.add(3); set2.add(4);
        set1.complement(set2);
        assertEquals(3, set1.length());
        assertEquals(3, set2.length());
    }

    @Test
    @DisplayName("Test complement() with identical sets returns empty set")
    public void testComplementIdentical() {
        set1.add(1); set1.add(2);
        set2.add(1); set2.add(2);
        IntegerSet result = set1.complement(set2);
        assertTrue(result.isEmpty());
    }

    // -------------------------------------------------------------------------
    // toString()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Test toString() returns sorted elements in correct format")
    public void testToStringSorted() {
        set1.add(3); set1.add(1); set1.add(2);
        assertEquals("[1, 2, 3]", set1.toString());
    }

    @Test
    @DisplayName("Test toString() on empty set returns []")
    public void testToStringEmpty() {
        assertEquals("[]", set1.toString());
    }

    @Test
    @DisplayName("Test toString() on single-element set")
    public void testToStringSingle() {
        set1.add(42);
        assertEquals("[42]", set1.toString());
    }
}

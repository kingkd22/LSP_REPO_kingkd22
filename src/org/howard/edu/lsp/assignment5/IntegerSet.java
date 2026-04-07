package org.howard.edu.lsp.assignment5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Models a mathematical set of integers. A set cannot contain duplicate values.
 * All set operations (union, intersect, diff, complement) return a new IntegerSet
 * and do not modify the original sets.
 */
public class IntegerSet {

    /** The backing list storing the elements of this set. */
    private ArrayList<Integer> set = new ArrayList<>();

    /** Constructs an empty IntegerSet. */
    public IntegerSet() {}

    /**
     * Removes all elements from this set.
     */
    public void clear() {
        set.clear();
    }

    /**
     * Returns the number of elements in this set.
     *
     * @return the cardinality of the set
     */
    public int length() {
        return set.size();
    }

    /**
     * Returns {@code true} if this set contains exactly the same elements as {@code b},
     * regardless of order.
     *
     * @param b the other IntegerSet to compare with
     * @return {@code true} if both sets contain the same elements
     */
    public boolean equals(IntegerSet b) {
        if (set.size() != b.set.size()) {
            return false;
        }
        return set.containsAll(b.set) && b.set.containsAll(set);
    }

    /**
     * Returns {@code true} if this set contains the specified value.
     *
     * @param value the integer to search for
     * @return {@code true} if the value is in the set
     */
    public boolean contains(int value) {
        return set.contains(value);
    }

    /**
     * Returns the largest element in this set.
     *
     * @return the largest integer in the set
     * @throws IntegerSetException if the set is empty
     */
    public int largest() {
        if (set.isEmpty()) {
            throw new IntegerSetException("Set is empty");
        }
        return Collections.max(set);
    }

    /**
     * Returns the smallest element in this set.
     *
     * @return the smallest integer in the set
     * @throws IntegerSetException if the set is empty
     */
    public int smallest() {
        if (set.isEmpty()) {
            throw new IntegerSetException("Set is empty");
        }
        return Collections.min(set);
    }

    /**
     * Adds the specified item to this set if it is not already present.
     *
     * @param item the integer to add
     */
    public void add(int item) {
        if (!contains(item)) {
            set.add(item);
        }
    }

    /**
     * Removes the specified item from this set if it is present.
     * Has no effect if the item is not in the set.
     *
     * @param item the integer to remove
     */
    public void remove(int item) {
        set.remove(Integer.valueOf(item));
    }

    /**
     * Returns a new IntegerSet containing all elements that appear in either
     * this set or {@code intSetb}.
     *
     * @param intSetb the other IntegerSet
     * @return a new set representing the union
     */
    public IntegerSet union(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        for (int item : intSetb.set) {
            if (!result.set.contains(item)) {
                result.set.add(item);
            }
        }
        return result;
    }

    /**
     * Returns a new IntegerSet containing only elements common to both
     * this set and {@code intSetb}.
     *
     * @param intSetb the other IntegerSet
     * @return a new set representing the intersection
     */
    public IntegerSet intersect(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        result.set.retainAll(intSetb.set);
        return result;
    }

    /**
     * Returns a new IntegerSet containing elements in this set that are not in {@code intSetb}.
     *
     * @param intSetb the other IntegerSet
     * @return a new set representing the difference (this − intSetb)
     */
    public IntegerSet diff(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(this.set);
        result.set.removeAll(intSetb.set);
        return result;
    }

    /**
     * Returns a new IntegerSet containing elements in {@code intSetb} that are not in this set.
     *
     * @param intSetb the other IntegerSet
     * @return a new set representing the complement (intSetb − this)
     */
    public IntegerSet complement(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        result.set.addAll(intSetb.set);
        result.set.removeAll(this.set);
        return result;
    }

    /**
     * Returns {@code true} if this set contains no elements.
     *
     * @return {@code true} if the set is empty
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * Returns a string representation of this set. Elements appear in ascending order,
     * separated by a comma and a single space, enclosed in square brackets.
     * An empty set is represented as {@code []}.
     *
     * @return string representation, e.g. {@code [1, 2, 3]} or {@code []}
     */
    @Override
    public String toString() {
        List<Integer> sorted = new ArrayList<>(set);
        Collections.sort(sorted);
        return sorted.toString();
    }
}

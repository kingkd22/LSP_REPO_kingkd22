package org.howard.edu.lsp.assignment5;

/**
 * Custom checked exception thrown when an operation is performed on an empty IntegerSet
 * that requires at least one element (e.g., largest(), smallest()).
 */
public class IntegerSetException extends RuntimeException {

    /**
     * Constructs an IntegerSetException with the specified detail message.
     *
     * @param message the detail message
     */
    public IntegerSetException(String message) {
        super(message);
    }
}

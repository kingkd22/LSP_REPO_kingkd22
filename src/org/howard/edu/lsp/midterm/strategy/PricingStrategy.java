package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy interface for pricing discount algorithms.
 *
 * <p>Each concrete implementation encapsulates a specific discount rule for a
 * particular customer type, allowing {@link PriceCalculator} to delegate the
 * calculation without knowing which algorithm is in use.</p>
 *
 * @author Kingston Davies
 */
public interface PricingStrategy {

    /**
     * Applies the discount rule to the given base price and returns the final price.
     *
     * @param price the original price before any discount is applied
     * @return the final price after applying the discount
     */
    double applyDiscount(double price);
}

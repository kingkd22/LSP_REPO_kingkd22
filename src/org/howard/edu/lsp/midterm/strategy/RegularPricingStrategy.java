package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for regular customers.
 *
 * <p>No discount is applied; the price is returned unchanged.</p>
 *
 * @author Kingston Davies
 */
public class RegularPricingStrategy implements PricingStrategy {

    /**
     * Returns the original price with no discount applied.
     *
     * @param price the original price
     * @return the original price unchanged
     */
    @Override
    public double applyDiscount(double price) {
        return price;
    }
}

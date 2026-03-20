package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for member customers.
 *
 * <p>Applies a 10% discount to the original price.</p>
 *
 * @author Kingston Davies
 */
public class MemberPricingStrategy implements PricingStrategy {

    /**
     * Applies a 10% discount to the given price.
     *
     * @param price the original price
     * @return the price after a 10% discount (price * 0.90)
     */
    @Override
    public double applyDiscount(double price) {
        return price * 0.90;
    }
}

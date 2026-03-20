package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for VIP customers.
 *
 * <p>Applies a 20% discount to the original price.</p>
 *
 * @author Kingston Davies
 */
public class VIPPricingStrategy implements PricingStrategy {

    /**
     * Applies a 20% discount to the given price.
     *
     * @param price the original price
     * @return the price after a 20% discount (price * 0.80)
     */
    @Override
    public double applyDiscount(double price) {
        return price * 0.80;
    }
}

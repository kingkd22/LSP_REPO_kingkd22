package org.howard.edu.lsp.midterm.strategy;

/**
 * Pricing strategy for holiday promotions.
 *
 * <p>Applies a 15% discount to the original price.</p>
 *
 * @author Kingston Davies
 */
public class HolidayPricingStrategy implements PricingStrategy {

    /**
     * Applies a 15% discount to the given price.
     *
     * @param price the original price
     * @return the price after a 15% discount (price * 0.85)
     */
    @Override
    public double applyDiscount(double price) {
        return price * 0.85;
    }
}

package org.howard.edu.lsp.midterm.strategy;

/**
 * Context class that calculates the final price for a customer purchase.
 *
 * <p>Rather than embedding discount logic directly, this class delegates to a
 * {@link PricingStrategy} supplied at construction time. Adding a new customer
 * type requires only a new {@code PricingStrategy} implementation — this class
 * never needs to change.</p>
 *
 * @author Kingston Davies
 */
public class PriceCalculator {

    /** The pricing strategy used to compute the final price. */
    private final PricingStrategy strategy;

    /**
     * Constructs a {@code PriceCalculator} with the given pricing strategy.
     *
     * @param strategy the discount strategy to apply; must not be {@code null}
     */
    public PriceCalculator(PricingStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Calculates the final price by delegating to the configured strategy.
     *
     * @param price the original purchase price
     * @return the final price after the strategy's discount has been applied
     */
    public double calculatePrice(double price) {
        return strategy.applyDiscount(price);
    }
}

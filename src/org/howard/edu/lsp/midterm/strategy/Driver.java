package org.howard.edu.lsp.midterm.strategy;

/**
 * Driver program demonstrating the Strategy Pattern implementation of
 * {@link PriceCalculator}.
 *
 * <p>Each customer type is paired with its corresponding {@link PricingStrategy},
 * and the final price is computed via {@link PriceCalculator#calculatePrice(double)}
 * using a base price of 100.0.</p>
 *
 * <p>Expected output:
 * <pre>
 * REGULAR: 100.0
 * MEMBER: 90.0
 * VIP: 80.0
 * HOLIDAY: 85.0
 * </pre>
 * </p>
 *
 * @author Kingston Davies
 */
public class Driver {

    /**
     * Entry point for the driver program.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        final double basePrice = 100.0;

        PriceCalculator regular = new PriceCalculator(new RegularPricingStrategy());
        PriceCalculator member  = new PriceCalculator(new MemberPricingStrategy());
        PriceCalculator vip     = new PriceCalculator(new VIPPricingStrategy());
        PriceCalculator holiday = new PriceCalculator(new HolidayPricingStrategy());

        System.out.println("REGULAR: " + regular.calculatePrice(basePrice));
        System.out.println("MEMBER: "  + member.calculatePrice(basePrice));
        System.out.println("VIP: "     + vip.calculatePrice(basePrice));
        System.out.println("HOLIDAY: " + holiday.calculatePrice(basePrice));
    }
}

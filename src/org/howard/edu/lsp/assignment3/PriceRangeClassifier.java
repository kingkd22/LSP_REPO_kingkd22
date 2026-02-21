package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;

/**
 * Classifies a product price into one of four named price range labels.
 *
 * <p>Classification thresholds (all inclusive on the upper bound):</p>
 * <ul>
 *   <li>{@code "Low"}     &mdash; price &le; $10.00</li>
 *   <li>{@code "Medium"}  &mdash; $10.00 &lt; price &le; $100.00</li>
 *   <li>{@code "High"}    &mdash; $100.00 &lt; price &le; $500.00</li>
 *   <li>{@code "Premium"} &mdash; price &gt; $500.00</li>
 * </ul>
 *
 * <p>This class is stateless and a single instance may be shared freely across
 * threads and transformation rules.</p>
 */
public class PriceRangeClassifier {

    /** Upper bound (inclusive) for the {@code "Low"} price range. */
    private static final BigDecimal LOW_MAX    = new BigDecimal("10.00");

    /** Upper bound (inclusive) for the {@code "Medium"} price range. */
    private static final BigDecimal MEDIUM_MAX = new BigDecimal("100.00");

    /** Upper bound (inclusive) for the {@code "High"} price range. */
    private static final BigDecimal HIGH_MAX   = new BigDecimal("500.00");

    /**
     * Classifies the given price into one of four named price range labels.
     *
     * @param price the price to classify; must not be {@code null}
     * @return {@code "Low"} if price &le; $10.00;
     *         {@code "Medium"} if $10.00 &lt; price &le; $100.00;
     *         {@code "High"} if $100.00 &lt; price &le; $500.00;
     *         {@code "Premium"} if price &gt; $500.00
     */
    public String classify(BigDecimal price) {
        if (price.compareTo(LOW_MAX) <= 0) {
            return "Low";
        } else if (price.compareTo(MEDIUM_MAX) <= 0) {
            return "Medium";
        } else if (price.compareTo(HIGH_MAX) <= 0) {
            return "High";
        } else {
            return "Premium";
        }
    }
}

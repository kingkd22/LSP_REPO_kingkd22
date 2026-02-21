package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;

/**
 * Upgrades the category of high-value Electronics products to {@code "Premium Electronics"}.
 *
 * <p>This rule applies when a product's category is {@code "Electronics"} and its
 * post-discount, rounded price exceeds {@code $500.00}. In that case the category
 * is changed to {@code "Premium Electronics"}. Products in other categories, or
 * Electronics products priced at or below the threshold, are not affected.</p>
 *
 * <p>This rule must be applied <em>after</em> both {@link ElectronicsDiscountRule}
 * and {@link PriceRoundingRule} so that the threshold comparison uses the
 * discounted, rounded price, faithfully replicating the original pipeline logic.</p>
 */
public class CategoryUpgradeRule implements TransformationRule {

    /** The source category eligible for an upgrade. */
    private static final String ELECTRONICS_CATEGORY = "Electronics";

    /** The upgraded category name assigned to qualifying products. */
    private static final String PREMIUM_CATEGORY = "Premium Electronics";

    /** The minimum price (exclusive) required to trigger the category upgrade. */
    private static final BigDecimal PREMIUM_THRESHOLD = new BigDecimal("500.00");

    /**
     * Upgrades the product's category from {@code "Electronics"} to
     * {@code "Premium Electronics"} if the post-discount, rounded price strictly
     * exceeds {@code $500.00}. All other fields are unchanged.
     *
     * @param product the product to evaluate; must not be {@code null}
     */
    @Override
    public void apply(Product product) {
        if (ELECTRONICS_CATEGORY.equals(product.getCategory())
                && product.getPrice().compareTo(PREMIUM_THRESHOLD) > 0) {
            product.setCategory(PREMIUM_CATEGORY);
        }
    }
}

package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;

/**
 * Applies a 10% discount to products in the {@code "Electronics"} category.
 *
 * <p>This rule checks whether a product's category is exactly {@code "Electronics"}.
 * If so, the product price is multiplied by {@code 0.90}. Products in any other
 * category are not affected.</p>
 *
 * <p>This rule does <em>not</em> round the result. Rounding is the sole
 * responsibility of {@link PriceRoundingRule}, which must follow this rule in
 * the transformation sequence so that the rounded price is available to both
 * {@link CategoryUpgradeRule} and {@link PriceRangeRule}.</p>
 */
public class ElectronicsDiscountRule implements TransformationRule {

    /** The category name that triggers this discount. */
    private static final String ELECTRONICS_CATEGORY = "Electronics";

    /** Multiplier representing a 10% reduction (i.e., 90% of original price). */
    private static final BigDecimal DISCOUNT_MULTIPLIER = new BigDecimal("0.90");

    /**
     * Multiplies the product price by {@code 0.90} if the product's category is
     * {@code "Electronics"}. All other fields are unchanged. No rounding is applied
     * here; rounding is handled by the subsequent {@link PriceRoundingRule}.
     *
     * @param product the product to evaluate and potentially discount; must not be {@code null}
     */
    @Override
    public void apply(Product product) {
        if (ELECTRONICS_CATEGORY.equals(product.getCategory())) {
            BigDecimal discounted = product.getPrice().multiply(DISCOUNT_MULTIPLIER);
            product.setPrice(discounted);
        }
    }
}

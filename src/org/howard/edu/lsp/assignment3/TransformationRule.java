package org.howard.edu.lsp.assignment3;

/**
 * Defines the contract for a single, self-contained transformation rule
 * applied to a {@link Product} during the Transform phase of the ETL pipeline.
 *
 * <p>Each implementation encapsulates exactly one business rule. Rules are
 * composed into an ordered sequence within a {@link ProductTransformer} and
 * applied one after another. Implementations must modify the product in place
 * using its setter methods.</p>
 *
 * <p>Rule ordering matters. For example, {@link ElectronicsDiscountRule} must
 * run before {@link PriceRoundingRule}, which in turn must run before
 * {@link CategoryUpgradeRule}, so that the category upgrade threshold check
 * reflects the discounted, rounded price.</p>
 *
 * @see ElectronicsDiscountRule
 * @see PriceRoundingRule
 * @see CategoryUpgradeRule
 * @see PriceRangeRule
 * @see ProductTransformer
 */
public interface TransformationRule {

    /**
     * Applies this transformation rule to the given product, modifying it in place.
     *
     * @param product the product to transform; must not be {@code null}
     */
    void apply(Product product);
}

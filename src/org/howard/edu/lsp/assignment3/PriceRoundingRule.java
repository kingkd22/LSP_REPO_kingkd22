package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Rounds every product's price to exactly 2 decimal places using
 * {@link RoundingMode#HALF_UP}.
 *
 * <p>This rule applies unconditionally to all products regardless of category.
 * It mirrors line 68 of the original {@code assignment2} pipeline, which rounds
 * the price for every row immediately after the optional Electronics discount
 * and before the category-upgrade and price-range checks.</p>
 *
 * <p>Correct placement in the rule sequence is therefore:</p>
 * <ol>
 *   <li>{@link ElectronicsDiscountRule} &mdash; apply discount (if applicable)</li>
 *   <li><b>This rule</b> &mdash; round to 2 dp for all products</li>
 *   <li>{@link CategoryUpgradeRule} &mdash; threshold check uses rounded price</li>
 *   <li>{@link PriceRangeRule} &mdash; label assigned from rounded price</li>
 * </ol>
 */
public class PriceRoundingRule implements TransformationRule {

    /**
     * Rounds the product's price to 2 decimal places using {@link RoundingMode#HALF_UP}.
     * All other product fields are unchanged.
     *
     * @param product the product whose price should be rounded; must not be {@code null}
     */
    @Override
    public void apply(Product product) {
        BigDecimal rounded = product.getPrice().setScale(2, RoundingMode.HALF_UP);
        product.setPrice(rounded);
    }
}

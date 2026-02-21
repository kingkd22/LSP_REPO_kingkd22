package org.howard.edu.lsp.assignment3;

/**
 * Assigns a {@code PriceRange} label to a product based on its final price.
 *
 * <p>This rule delegates the classification logic to a {@link PriceRangeClassifier}
 * and writes the result to the product via {@link Product#setPriceRange(String)}.
 * It should be the last rule in the transformation sequence so that the label
 * reflects the product's fully adjusted, rounded final price.</p>
 */
public class PriceRangeRule implements TransformationRule {

    /** Classifier used to determine the price range label from a price value. */
    private final PriceRangeClassifier classifier;

    /**
     * Constructs a {@code PriceRangeRule} backed by the given classifier.
     *
     * @param classifier the {@link PriceRangeClassifier} to use for label determination;
     *                   must not be {@code null}
     */
    public PriceRangeRule(PriceRangeClassifier classifier) {
        this.classifier = classifier;
    }

    /**
     * Determines the price range label for the product's current price and assigns
     * it via {@link Product#setPriceRange(String)}. This rule should run after all
     * price-modifying rules so that the label reflects the final price.
     *
     * @param product the product to label; must not be {@code null}
     */
    @Override
    public void apply(Product product) {
        String label = classifier.classify(product.getPrice());
        product.setPriceRange(label);
    }
}

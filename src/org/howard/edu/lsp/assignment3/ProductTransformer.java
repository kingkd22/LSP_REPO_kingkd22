package org.howard.edu.lsp.assignment3;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies an ordered sequence of {@link TransformationRule} objects to each
 * {@link Product}, implementing the Transform (T) phase of the ETL pipeline.
 *
 * <p>Rules are applied strictly in the order they were registered. The transformer
 * itself applies no additional logic beyond invoking each rule in sequence; all
 * transformation behaviour — including discounting, rounding, category upgrades,
 * and price-range labelling — is encapsulated in the individual rules.</p>
 *
 * <p>The default rule sequence configured by {@link ETLPipeline} is:</p>
 * <ol>
 *   <li>{@link ElectronicsDiscountRule} &mdash; 10% price reduction for Electronics</li>
 *   <li>{@link PriceRoundingRule} &mdash; rounds all prices to 2 decimal places</li>
 *   <li>{@link CategoryUpgradeRule} &mdash; upgrades high-value Electronics to Premium</li>
 *   <li>{@link PriceRangeRule} &mdash; assigns the PriceRange label</li>
 * </ol>
 *
 * <p>Custom rule sequences can be injected at construction time to support
 * alternative business logic or unit testing without file I/O.</p>
 */
public class ProductTransformer {

    /** Ordered list of transformation rules applied to each product. */
    private final List<TransformationRule> rules;

    /**
     * Constructs a {@code ProductTransformer} with the supplied ordered list of rules.
     * A defensive copy is made so that external modifications to the provided list
     * do not affect this transformer after construction.
     *
     * @param rules the ordered list of {@link TransformationRule} objects to apply;
     *              must not be {@code null}
     */
    public ProductTransformer(List<TransformationRule> rules) {
        this.rules = new ArrayList<>(rules);
    }

    /**
     * Applies all registered transformation rules to the given product in sequence.
     * The product is modified in place. The same reference is returned for
     * call-chaining convenience.
     *
     * @param product the product to transform; must not be {@code null}
     * @return the transformed product (same reference as the argument)
     */
    public Product transform(Product product) {
        for (TransformationRule rule : rules) {
            rule.apply(product);
        }
        return product;
    }

    /**
     * Applies {@link #transform(Product)} to every product in the given list.
     * Products are modified in place; the same list reference is returned.
     *
     * @param products the list of products to transform; must not be {@code null}
     * @return the same list with all products transformed in place
     */
    public List<Product> transformAll(List<Product> products) {
        for (Product product : products) {
            transform(product);
        }
        return products;
    }
}

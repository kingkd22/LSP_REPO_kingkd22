package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;

/**
 * Represents a single product record flowing through the ETL pipeline.
 *
 * <p>A {@code Product} is the central data object in the pipeline. It is produced
 * by a {@link ProductReader} during the Extract phase, mutated by a
 * {@link ProductTransformer} during the Transform phase, and consumed by a
 * {@link ProductWriter} during the Load phase.</p>
 *
 * <p>The {@code priceRange} field is not populated at construction time; it is
 * assigned by the {@link PriceRangeRule} during transformation.</p>
 */
public class Product {

    /** Unique numeric identifier for this product. */
    private int productId;

    /** Display name of the product. */
    private String name;

    /** Price of the product; may be modified during transformation. */
    private BigDecimal price;

    /** Category of the product (e.g., {@code "Electronics"}, {@code "Premium Electronics"}). */
    private String category;

    /** Price range label assigned during transformation (e.g., {@code "Low"}, {@code "High"}). */
    private String priceRange;

    /**
     * Constructs a new {@code Product} with the four fields extracted from a data source.
     * The {@code priceRange} field is left {@code null} until the transformation phase.
     *
     * @param productId the unique product identifier
     * @param name      the product name (caller is responsible for any normalisation)
     * @param price     the initial product price
     * @param category  the product category
     */
    public Product(int productId, String name, BigDecimal price, String category) {
        this.productId = productId;
        this.name = name;
        this.price = price;
        this.category = category;
    }

    /**
     * Returns the product's unique identifier.
     *
     * @return the product ID
     */
    public int getProductId() {
        return productId;
    }

    /**
     * Returns the product name.
     *
     * @return the product name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the product price.
     *
     * @return the price as a {@link BigDecimal}
     */
    public BigDecimal getPrice() {
        return price;
    }

    /**
     * Sets the product price. Used by transformation rules to apply discounts or rounding.
     *
     * @param price the new price; must not be {@code null}
     */
    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    /**
     * Returns the product category.
     *
     * @return the category string
     */
    public String getCategory() {
        return category;
    }

    /**
     * Sets the product category. Used by transformation rules to upgrade a category
     * (e.g., from {@code "Electronics"} to {@code "Premium Electronics"}).
     *
     * @param category the new category; must not be {@code null}
     */
    public void setCategory(String category) {
        this.category = category;
    }

    /**
     * Returns the price range label assigned during transformation.
     *
     * @return the price range label, or {@code null} if transformation has not yet run
     */
    public String getPriceRange() {
        return priceRange;
    }

    /**
     * Sets the price range label. Called by {@link PriceRangeRule} after the
     * product's final price has been determined.
     *
     * @param priceRange the label to assign (e.g., {@code "Low"}, {@code "Medium"},
     *                   {@code "High"}, {@code "Premium"})
     */
    public void setPriceRange(String priceRange) {
        this.priceRange = priceRange;
    }
}

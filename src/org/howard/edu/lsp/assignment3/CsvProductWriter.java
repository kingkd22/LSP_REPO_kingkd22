package org.howard.edu.lsp.assignment3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Writes transformed {@link Product} records to a CSV output file.
 *
 * <p>This class implements the Load (L) phase of the ETL pipeline for
 * comma-separated value output. It writes a fixed header row followed by
 * one row per product in the following format:</p>
 * <pre>
 *   ProductID,Name,Price,Category,PriceRange
 * </pre>
 * <p>The output file is created or overwritten on each call to
 * {@link #writeAll(List)}.</p>
 */
public class CsvProductWriter implements ProductWriter {

    /** Path to the CSV output file. */
    private final String filePath;

    /** CSV header written at the top of every output file. */
    private static final String HEADER = "ProductID,Name,Price,Category,PriceRange";

    /**
     * Constructs a {@code CsvProductWriter} targeting the specified output file path.
     * The file is created or overwritten when {@link #writeAll(List)} is called.
     *
     * @param filePath path to the output CSV file; must not be {@code null}
     */
    public CsvProductWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Writes all transformed products to the CSV output file.
     *
     * <p>A header row is always written first. Each product is then serialised
     * to a single comma-delimited line. The output file is created fresh on
     * every invocation; any existing file at the target path is overwritten.</p>
     *
     * @param products the list of transformed {@link Product} objects to write;
     *                 must not be {@code null}
     * @throws IOException if the output file cannot be created or written to
     */
    @Override
    public void writeAll(List<Product> products) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(HEADER);
            writer.newLine();

            for (Product product : products) {
                writer.write(serialize(product));
                writer.newLine();
            }
        }
    }

    /**
     * Serialises a single {@link Product} to a CSV-formatted string.
     * Fields are written in the order: ProductID, Name, Price, Category, PriceRange.
     *
     * @param product the product to serialise; must not be {@code null}
     * @return a comma-separated string representation of the product
     */
    private String serialize(Product product) {
        return product.getProductId() + "," +
               product.getName()      + "," +
               product.getPrice().toString() + "," +
               product.getCategory()  + "," +
               product.getPriceRange();
    }
}

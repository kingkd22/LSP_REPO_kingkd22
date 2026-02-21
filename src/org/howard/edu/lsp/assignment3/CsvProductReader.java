package org.howard.edu.lsp.assignment3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads product records from a CSV file and parses them into {@link Product} objects.
 *
 * <p>This class implements the Extract (E) phase of the ETL pipeline for
 * comma-separated value files. The expected column format is:</p>
 * <pre>
 *   ProductID,Name,Price,Category
 * </pre>
 * <p>The first row is always treated as a header and discarded. Rows that are
 * blank, contain the wrong number of fields, or have non-numeric {@code ProductID}
 * or {@code Price} values are silently skipped. Skipped and read counts are
 * tracked internally and available after {@link #readAll()} returns.</p>
 *
 * <p>Product names are uppercased during parsing as a normalisation step.</p>
 */
public class CsvProductReader implements ProductReader {

    /** Path to the CSV input file. */
    private final String filePath;

    /** Total data rows encountered during the most recent {@link #readAll()} call, excluding the header. */
    private int rowsRead;

    /** Rows skipped due to validation or parsing failures during the most recent {@link #readAll()} call. */
    private int rowsSkipped;

    /** Number of comma-separated fields expected on each data row. */
    private static final int EXPECTED_FIELD_COUNT = 4;

    /**
     * Constructs a {@code CsvProductReader} targeting the specified file path.
     *
     * @param filePath path to the CSV file to read; must not be {@code null}
     */
    public CsvProductReader(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Reads and parses all valid product records from the CSV file.
     *
     * <p>The header row is consumed and discarded. Each subsequent row is
     * trimmed, validated for field count, and parsed into a {@link Product}.
     * Blank lines, rows with the wrong number of fields, and rows with
     * non-numeric {@code ProductID} or {@code Price} values increment the
     * skipped counter and are excluded from the returned list. Product names
     * are uppercased during parsing.</p>
     *
     * @return a list of valid {@link Product} objects; never {@code null},
     *         but may be empty if the file is empty or all rows are invalid
     * @throws IOException if the file does not exist or cannot be read
     */
    @Override
    public List<Product> readAll() throws IOException {
        List<Product> products = new ArrayList<>();
        rowsRead = 0;
        rowsSkipped = 0;

        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("Input file not found at " + filePath);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine(); // consume header row
            if (line == null) {
                return products; // empty file
            }

            while ((line = reader.readLine()) != null) {
                rowsRead++;

                line = line.trim();
                if (line.isEmpty()) {
                    rowsSkipped++;
                    continue;
                }

                String[] fields = line.split(",");
                if (fields.length != EXPECTED_FIELD_COUNT) {
                    rowsSkipped++;
                    continue;
                }

                try {
                    int productId     = Integer.parseInt(fields[0].trim());
                    String name       = fields[1].trim().toUpperCase();
                    BigDecimal price  = new BigDecimal(fields[2].trim());
                    String category   = fields[3].trim();

                    products.add(new Product(productId, name, price, category));
                } catch (NumberFormatException e) {
                    rowsSkipped++;
                }
            }
        }

        return products;
    }

    /**
     * Returns the total number of data rows encountered during the most recent
     * call to {@link #readAll()}, excluding the header row. This count includes
     * both successfully parsed rows and skipped rows.
     *
     * @return total data rows read
     */
    @Override
    public int getRowsRead() {
        return rowsRead;
    }

    /**
     * Returns the number of rows skipped due to validation or parsing failures
     * during the most recent call to {@link #readAll()}.
     *
     * @return number of skipped rows
     */
    @Override
    public int getRowsSkipped() {
        return rowsSkipped;
    }
}

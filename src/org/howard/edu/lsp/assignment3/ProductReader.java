package org.howard.edu.lsp.assignment3;

import java.io.IOException;
import java.util.List;

/**
 * Defines the contract for reading product records from a data source.
 *
 * <p>Implementations handle the Extract (E) phase of the ETL pipeline.
 * They are responsible for source-specific concerns — file I/O, parsing, and
 * row-level validation — and return only valid {@link Product} objects.
 * Statistics about rows read and rows skipped are tracked internally and
 * exposed via {@link #getRowsRead()} and {@link #getRowsSkipped()}.</p>
 *
 * @see CsvProductReader
 */
public interface ProductReader {

    /**
     * Reads all valid product records from the underlying data source.
     *
     * <p>Invalid rows (e.g., wrong field count, non-numeric values) are silently
     * skipped and counted. Only successfully parsed records are returned.</p>
     *
     * @return a list of valid {@link Product} objects; never {@code null}
     * @throws IOException if the data source cannot be opened or read
     */
    List<Product> readAll() throws IOException;

    /**
     * Returns the total number of data rows encountered, excluding any header row.
     * This count includes both successfully parsed rows and skipped rows.
     *
     * @return total data rows read
     */
    int getRowsRead();

    /**
     * Returns the number of rows that were skipped due to validation or parsing failures.
     *
     * @return number of skipped rows
     */
    int getRowsSkipped();
}

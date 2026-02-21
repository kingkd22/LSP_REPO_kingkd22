package org.howard.edu.lsp.assignment3;

import java.io.IOException;
import java.util.List;

/**
 * Defines the contract for writing transformed product records to a data sink.
 *
 * <p>Implementations handle the Load (L) phase of the ETL pipeline.
 * They are responsible for sink-specific concerns such as file I/O and
 * record serialisation. Implementations should write any required header or
 * schema information before writing data rows.</p>
 *
 * @see CsvProductWriter
 */
public interface ProductWriter {

    /**
     * Writes a list of transformed {@link Product} objects to the data sink.
     *
     * @param products the list of products to write; must not be {@code null}
     * @throws IOException if the sink cannot be created, opened, or written to
     */
    void writeAll(List<Product> products) throws IOException;
}

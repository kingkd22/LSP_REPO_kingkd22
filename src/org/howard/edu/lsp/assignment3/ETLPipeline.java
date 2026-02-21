package org.howard.edu.lsp.assignment3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Orchestrates the ETL (Extract, Transform, Load) pipeline for product data.
 *
 * <p>This class acts as a thin coordinator that wires together the pipeline
 * components and drives execution in three sequential phases:</p>
 * <ol>
 *   <li><b>Extract</b> &mdash; {@link ProductReader} reads and parses all valid
 *       records from the data source.</li>
 *   <li><b>Transform</b> &mdash; {@link ProductTransformer} applies the registered
 *       {@link TransformationRule} objects to every record.</li>
 *   <li><b>Load</b> &mdash; {@link ProductWriter} serialises and writes the
 *       transformed records to the data sink.</li>
 * </ol>
 *
 * <p>All business logic, I/O, and reporting concerns are delegated to the
 * injected collaborators. {@code ETLPipeline} itself contains no parsing,
 * no transformation rules, and no file handling code.</p>
 *
 * <p>The static {@link #main(String[])} method constructs a default pipeline
 * configuration (CSV in, CSV out, standard transformation rules) and calls
 * {@link #run()}, preserving the same entry point and behaviour as the
 * original {@code assignment2} implementation.</p>
 */
public class ETLPipeline {

    /** Default path for the CSV input file. */
    private static final String INPUT_PATH  = "data/products.csv";

    /** Default path for the CSV output file. */
    private static final String OUTPUT_PATH = "data/transformed_products.csv";

    /** Reader responsible for the Extract phase. */
    private final ProductReader reader;

    /** Transformer responsible for the Transform phase. */
    private final ProductTransformer transformer;

    /** Writer responsible for the Load phase. */
    private final ProductWriter writer;

    /** Reporter that accumulates and prints pipeline statistics. */
    private final PipelineReporter reporter;

    /**
     * Constructs an {@code ETLPipeline} with the given collaborators.
     * Dependencies are injected rather than created internally, making
     * the pipeline independently testable and extensible.
     *
     * @param reader      the {@link ProductReader} for the Extract phase; must not be {@code null}
     * @param transformer the {@link ProductTransformer} for the Transform phase; must not be {@code null}
     * @param writer      the {@link ProductWriter} for the Load phase; must not be {@code null}
     * @param reporter    the {@link PipelineReporter} for statistics tracking; must not be {@code null}
     */
    public ETLPipeline(ProductReader reader,
                       ProductTransformer transformer,
                       ProductWriter writer,
                       PipelineReporter reporter) {
        this.reader      = reader;
        this.transformer = transformer;
        this.writer      = writer;
        this.reporter    = reporter;
    }

    /**
     * Executes the full ETL pipeline: extract, transform, load, then report.
     *
     * <p>Specifically:</p>
     * <ol>
     *   <li>Delegates to the {@link ProductReader} to extract all valid records.</li>
     *   <li>Passes the resulting list to {@link ProductTransformer#transformAll(List)}.</li>
     *   <li>Passes the transformed list to {@link ProductWriter#writeAll(List)}.</li>
     *   <li>Populates the {@link PipelineReporter} with read, written, and skipped counts,
     *       then calls {@link PipelineReporter#printSummary()}.</li>
     * </ol>
     *
     * <p>If the input file is not found, an error message is printed to standard
     * output and no output file is created. If a write error occurs, an error
     * message is printed and the output file may be empty or incomplete.</p>
     */
    public void run() {
        List<Product> products;

        try {
            products = reader.readAll();
        } catch (IOException e) {
            System.out.println("ERROR: " + e.getMessage());
            return;
        }

        transformer.transformAll(products);

        try {
            writer.writeAll(products);
        } catch (IOException e) {
            System.out.println("ERROR: File processing failed.");
            return;
        }

        reporter.setRowsRead(reader.getRowsRead());
        reporter.setRowsWritten(products.size());
        reporter.setRowsSkipped(reader.getRowsSkipped());
        reporter.printSummary();
    }

    /**
     * Entry point for the ETL pipeline application.
     *
     * <p>Constructs a default pipeline configuration using CSV I/O and the
     * standard transformation rule set, then calls {@link #run()}. The default
     * rule sequence mirrors the original {@code assignment2} logic exactly:</p>
     * <ol>
     *   <li>{@link ElectronicsDiscountRule} &mdash; apply 10% discount</li>
     *   <li>{@link PriceRoundingRule} &mdash; round all prices to 2 dp</li>
     *   <li>{@link CategoryUpgradeRule} &mdash; upgrade category on rounded price</li>
     *   <li>{@link PriceRangeRule} &mdash; assign label on rounded price</li>
     * </ol>
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        PriceRangeClassifier classifier = new PriceRangeClassifier();

        List<TransformationRule> rules = Arrays.asList(
                new ElectronicsDiscountRule(),
                new PriceRoundingRule(),
                new CategoryUpgradeRule(),
                new PriceRangeRule(classifier)
        );

        ETLPipeline pipeline = new ETLPipeline(
                new CsvProductReader(INPUT_PATH),
                new ProductTransformer(rules),
                new CsvProductWriter(OUTPUT_PATH),
                new PipelineReporter(OUTPUT_PATH)
        );

        pipeline.run();
    }
}

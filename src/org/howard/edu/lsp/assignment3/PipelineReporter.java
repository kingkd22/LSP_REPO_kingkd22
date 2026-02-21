package org.howard.edu.lsp.assignment3;

/**
 * Tracks and reports execution statistics for an ETL pipeline run.
 *
 * <p>This class accumulates counts of rows read, transformed, and skipped
 * during a pipeline execution. At the end of the run, {@link #printSummary()}
 * produces the same console output as the original {@code assignment2 ETLPipeline},
 * keeping reporting logic isolated from all I/O and transformation concerns.</p>
 *
 * <p>Counts can be supplied in two ways. Use the bulk setters
 * ({@link #setRowsRead}, {@link #setRowsWritten}, {@link #setRowsSkipped}) when
 * the final counts are already known after a phase completes — as {@link ETLPipeline}
 * does. Use the per-row incrementers ({@link #incrementRead},
 * {@link #incrementWritten}, {@link #incrementSkipped}) when counts must be
 * accumulated one row at a time during processing.</p>
 */
public class PipelineReporter {

    /** Total data rows read from the source (excluding header). */
    private int rowsRead;

    /** Rows successfully transformed and written to the output. */
    private int rowsWritten;

    /** Rows skipped due to validation or parsing failures. */
    private int rowsSkipped;

    /** Path of the output file, included in the summary for user reference. */
    private final String outputPath;

    /**
     * Constructs a {@code PipelineReporter} for the given output path.
     * All counters start at zero.
     *
     * @param outputPath the output file path displayed in the summary; must not be {@code null}
     */
    public PipelineReporter(String outputPath) {
        this.outputPath = outputPath;
    }

    /**
     * Sets the total number of data rows read from the source.
     *
     * @param rowsRead the read count; must be &ge; 0
     */
    public void setRowsRead(int rowsRead) {
        this.rowsRead = rowsRead;
    }

    /**
     * Sets the number of rows successfully written to the output.
     *
     * @param rowsWritten the written count; must be &ge; 0
     */
    public void setRowsWritten(int rowsWritten) {
        this.rowsWritten = rowsWritten;
    }

    /**
     * Sets the number of rows skipped due to validation or parsing failures.
     *
     * @param rowsSkipped the skipped count; must be &ge; 0
     */
    public void setRowsSkipped(int rowsSkipped) {
        this.rowsSkipped = rowsSkipped;
    }

    /**
     * Increments the rows-read counter by one. Use this when accumulating the
     * count row-by-row during processing. Use {@link #setRowsRead(int)} instead
     * when the final count is already known after a phase completes.
     */
    public void incrementRead() {
        rowsRead++;
    }

    /**
     * Increments the rows-written counter by one. Use this when accumulating the
     * count row-by-row during processing. Use {@link #setRowsWritten(int)} instead
     * when the final count is already known after a phase completes.
     */
    public void incrementWritten() {
        rowsWritten++;
    }

    /**
     * Increments the rows-skipped counter by one. Use this when accumulating the
     * count row-by-row during processing. Use {@link #setRowsSkipped(int)} instead
     * when the final count is already known after a phase completes.
     */
    public void incrementSkipped() {
        rowsSkipped++;
    }

    /**
     * Prints a summary of pipeline execution statistics to standard output.
     * The format matches the original {@code ETLPipeline} output exactly:
     * <pre>
     *   Rows read: &lt;n&gt;
     *   Rows transformed: &lt;n&gt;
     *   Rows skipped: &lt;n&gt;
     *   Output written to: &lt;path&gt;
     * </pre>
     */
    public void printSummary() {
        System.out.println("Rows read: "         + rowsRead);
        System.out.println("Rows transformed: "  + rowsWritten);
        System.out.println("Rows skipped: "      + rowsSkipped);
        System.out.println("Output written to: " + outputPath);
    }
}

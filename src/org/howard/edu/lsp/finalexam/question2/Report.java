package org.howard.edu.lsp.finalexam.question2;

/**
 * Abstract base class defining the Template Method pattern for report generation.
 * Subclasses must implement loadData, formatHeader, formatBody, and formatFooter.
 */
public abstract class Report {

    /**
     * Template method that defines the fixed report generation workflow.
     * Subclasses cannot override this method.
     */
    public final void generateReport() {
        loadData();
        System.out.println("=== HEADER ===");
        System.out.println(formatHeader());
        System.out.println("=== BODY ===");
        System.out.println(formatBody());
        System.out.println("=== FOOTER ===");
        System.out.println(formatFooter());
    }

    /**
     * Loads data required by the report. Must be implemented by subclasses.
     */
    protected abstract void loadData();

    /**
     * Returns the formatted header string for the report.
     */
    protected abstract String formatHeader();

    /**
     * Returns the formatted body string for the report.
     */
    protected abstract String formatBody();

    /**
     * Returns the formatted footer string for the report.
     */
    protected abstract String formatFooter();
}

package org.howard.edu.lsp.finalexam.question2;

/**
 * Concrete report for course data. Implements the variable steps of the Template Method.
 */
public class CourseReport extends Report {

    private String courseName;
    private int enrollment;

    /**
     * Loads course data used throughout the report.
     */
    @Override
    protected void loadData() {
        courseName = "CSCI 363";
        enrollment = 45;
    }

    @Override
    protected String formatHeader() {
        return "Course Report";
    }

    @Override
    protected String formatBody() {
        return "Course: " + courseName + "\nEnrollment: " + enrollment;
    }

    @Override
    protected String formatFooter() {
        return "End of Course Report";
    }
}

package org.howard.edu.lsp.finalexam.question2;

/**
 * Concrete report for student data. Implements the variable steps of the Template Method.
 */
public class StudentReport extends Report {

    private String studentName;
    private double gpa;

    /**
     * Loads student data used throughout the report.
     */
    @Override
    protected void loadData() {
        studentName = "John Doe";
        gpa = 3.8;
    }

    @Override
    protected String formatHeader() {
        return "Student Report";
    }

    @Override
    protected String formatBody() {
        return "Student Name: " + studentName + "\nGPA: " + gpa;
    }

    @Override
    protected String formatFooter() {
        return "End of Student Report";
    }
}

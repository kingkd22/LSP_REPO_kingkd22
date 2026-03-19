package org.howard.edu.lsp.midterm.crccards;

import java.util.Set;

/**
 * Represents a single task in the Task Management System.
 * Stores task information, tracks task status, and provides task details.
 *
 * @author Kingston Davies
 */
public class Task {

    private static final Set<String> VALID_STATUSES = Set.of("OPEN", "IN_PROGRESS", "COMPLETE");

    private String taskId;
    private String description;
    private String status;

    /**
     * Constructs a new Task with the given ID and description.
     * The default status is set to "OPEN".
     *
     * @param taskId      the unique identifier for this task
     * @param description a brief description of the task
     */
    public Task(String taskId, String description) {
        this.taskId = taskId;
        this.description = description;
        this.status = "OPEN";
    }

    /**
     * Returns the unique identifier of this task.
     *
     * @return the task ID
     */
    public String getTaskId() {
        return taskId;
    }

    /**
     * Returns the description of this task.
     *
     * @return the task description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the current status of this task.
     *
     * @return the task status (OPEN, IN_PROGRESS, COMPLETE, or UNKNOWN)
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status of this task.
     * Valid values are "OPEN", "IN_PROGRESS", and "COMPLETE" (case-sensitive).
     * If the provided value is not one of the valid statuses, the status is set to "UNKNOWN".
     *
     * @param status the new status to assign to this task
     */
    public void setStatus(String status) {
        if (VALID_STATUSES.contains(status)) {
            this.status = status;
        } else {
            this.status = "UNKNOWN";
        }
    }

    /**
     * Returns a string representation of this task in the format:
     * {@code taskId description [status]}
     * <p>Example: {@code T1 Write report [OPEN]}</p>
     *
     * @return formatted string representation of the task
     */
    @Override
    public String toString() {
        return taskId + " " + description + " [" + status + "]";
    }
}

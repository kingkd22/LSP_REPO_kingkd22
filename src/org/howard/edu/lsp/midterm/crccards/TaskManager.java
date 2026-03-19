package org.howard.edu.lsp.midterm.crccards;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages a collection of {@link Task} objects for the Task Management System.
 * Supports adding tasks, finding tasks by ID, and retrieving tasks by status.
 * Uses a {@link LinkedHashMap} internally to ensure unique task IDs, O(1) lookup by ID, and insertion-order iteration.
 *
 * @author Kingston Davies
 */
public class TaskManager {

    private Map<String, Task> tasks;

    /**
     * Constructs a new TaskManager with an empty task collection.
     */
    public TaskManager() {
        tasks = new LinkedHashMap<>();
    }

    /**
     * Adds a task to the manager.
     * Duplicate task IDs are not permitted.
     *
     * @param task the task to add
     * @throws IllegalArgumentException if a task with the same ID already exists
     */
    public void addTask(Task task) {
        if (tasks.containsKey(task.getTaskId())) {
            throw new IllegalArgumentException(
                "Task with ID '" + task.getTaskId() + "' already exists.");
        }
        tasks.put(task.getTaskId(), task);
    }

    /**
     * Finds and returns the task with the specified ID.
     *
     * @param taskId the ID of the task to find
     * @return the {@link Task} with the given ID, or {@code null} if not found
     */
    public Task findTask(String taskId) {
        return tasks.get(taskId);
    }

    /**
     * Returns a list of all tasks whose status matches the specified value.
     * The comparison is case-sensitive.
     *
     * @param status the status to filter by (e.g., "OPEN", "IN_PROGRESS", "COMPLETE")
     * @return a {@link List} of tasks matching the given status; empty list if none match
     */
    public List<Task> getTasksByStatus(String status) {
        List<Task> result = new ArrayList<>();
        for (Task task : tasks.values()) {
            if (task.getStatus().equals(status)) {
                result.add(task);
            }
        }
        return result;
    }
}

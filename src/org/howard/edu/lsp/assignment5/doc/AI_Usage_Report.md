# AI Usage Report — Assignment 5

## AI Tools Used

**Tool:** Claude (Anthropic) via Claude Code CLI

## Prompt Used

> "I need to implement a Java class called `IntegerSet`. The class models a mathematical set of integers (no duplicates) using an `ArrayList<Integer>`. It must include the following methods: `clear`, `length`, `isEmpty`, `contains`, `add`, `remove`, `largest`, `smallest`, `equals`, `union`, `intersect`, `diff`, `complement`, and `toString`. Set operations (`union`, `intersect`, `diff`, `complement`) must return a new `IntegerSet` without modifying the originals. `largest()` and `smallest()` must throw a custom exception called `IntegerSetException` when the set is empty. `toString()` must return elements sorted in ascending order in the format `[1, 2, 3]` or `[]` for empty. Also create JUnit 5 tests covering all methods including edge cases. The package is `org.howard.edu.lsp.assignment5`."

## External References Used

- Java SE Documentation — `ArrayList`, `Collections.max`, `Collections.min`, `Collections.sort`:
  https://docs.oracle.com/en/java/docs/

- JUnit 5 User Guide (for `@Test`, `@BeforeEach`, `assertThrows`, etc.):
  https://junit.org/junit5/docs/current/user-guide/

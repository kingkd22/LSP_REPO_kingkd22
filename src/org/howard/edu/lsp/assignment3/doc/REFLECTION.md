# Reflection: Assignment 2 vs Assignment 3

## What Changed

Assignment 2 solved the ETL problem in a single `ETLPipeline` class with one `main()` method spanning roughly 100 lines. File opening, header writing, row parsing, business rule evaluation, output formatting, and summary printing were all interleaved in a single procedural block. The only structural separation was a private `printSummary()` helper method.

Assignment 3 decomposes that same logic into 14 classes across three layers:

| Layer | Classes |
|---|---|
| Data model | `Product` |
| Interfaces | `ProductReader`, `ProductWriter`, `TransformationRule` |
| Implementations | `CsvProductReader`, `CsvProductWriter`, `ElectronicsDiscountRule`, `PriceRoundingRule`, `CategoryUpgradeRule`, `PriceRangeRule` |
| Support | `PriceRangeClassifier`, `ProductTransformer`, `PipelineReporter` |
| Orchestration | `ETLPipeline` |

Each class has a single, named responsibility. `ETLPipeline.run()` shrinks to a five-line
sequence — read, transform, write, report — with no business logic or I/O details inside it.

---

## How Assignment 3 Is More Object-Oriented

**Objects and classes.** In Assignment 2, a product is an anonymous collection of local variables (`productId`, `name`, `price`, `category`). There is no type that represents a product — data exists only as primitives scattered across the stack frame. Assignment 3 introduces `Product` as a first-class object. It carries its own state, moves through the pipeline as a named entity, and can
be reasoned about independently of the code that created or transformed it.

**Encapsulation.** Each class hides its implementation behind a public interface. `CsvProductReader` owns the `BufferedReader` lifecycle, field-count validation, and name uppercasing — none of that leaks into `ETLPipeline`. `PriceRangeClassifier` owns the four threshold constants; callers pass a `BigDecimal` and receive a label without knowing the cutoffs. `PipelineReporter` owns its counters and the exact format of the summary output. In Assignment 2 all of these details were visible in one place and could not be changed independently.

**Polymorphism.** Three interfaces (`ProductReader`, `ProductWriter`, `TransformationRule`) allow `ETLPipeline` to depend on abstractions rather than concrete classes. The orchestrator never imports `CsvProductReader` or `CsvProductWriter`; it works through the interfaces. This means a `DatabaseProductReader` or `JsonProductWriter` could be substituted without touching `ETLPipeline` at all. The `TransformationRule` interface applies the same idea to individual business rules: each rule is a distinct object implementing a single `apply(Product)` method. `ProductTransformer` iterates a `List<TransformationRule>` and calls `apply()` on each — polymorphic dispatch at work.

**Inheritance (via interface implementation).** Java does not support multiple class inheritance, but interfaces provide the same contract-based mechanism. `CsvProductReader implements ProductReader`, `CsvProductWriter implements ProductWriter`, and all four rule classes implement `TransformationRule`. Each concrete class inherits the contract of its interface and is substitutable wherever that interface is expected — satisfying the Liskov Substitution Principle.

---

## OO Concepts Applied — Summary

| Concept | Where applied |
|---|---|
| **Object** | `Product` — a typed, stateful entity flowing through the pipeline |
| **Class** | 14 classes, each with a single named responsibility |
| **Encapsulation** | `CsvProductReader` hides I/O; `PriceRangeClassifier` hides thresholds; `Product` hides fields behind getters/setters |
| **Polymorphism** | `ETLPipeline` programs to `ProductReader` / `ProductWriter` / `TransformationRule` interfaces; rules are interchangeable |
| **Inheritance** | All concrete implementations inherit their contracts from interfaces (`implements`) |

---

## Testing That Both Assignments Produce Identical Output

To verify correctness, both pipelines were compiled and run against the same input file
(`data/products.csv`) from the project root so that relative paths resolved identically.

```bash
# Compile
javac -d bin src/org/howard/edu/lsp/assignment2/ETLPipeline.java
javac -d bin src/org/howard/edu/lsp/assignment3/*.java

# Run Assignment 2 and save output
java -cp bin org.howard.edu.lsp.assignment2.ETLPipeline
cp data/transformed_products.csv data/a2_output.csv

# Run Assignment 3 and save output
java -cp bin org.howard.edu.lsp.assignment3.ETLPipeline
cp data/transformed_products.csv data/a3_output.csv

# Compare
diff data/a2_output.csv data/a3_output.csv && echo "IDENTICAL"
```

Both pipelines produced the same console output:

```
Rows read: 11
Rows transformed: 7
Rows skipped: 4
Output written to: data/transformed_products.csv
```

The `diff` command reported no differences — the output CSV files were byte-for-byte identical
across all eight lines (header plus seven data rows).

Three subtle compatibility issues were caught and fixed during development:

1. **Error message wording** — `CsvProductReader` initially used `"found: "` instead of
   `"found at "`, producing a different missing-file error string than Assignment 2.
2. **Price serialisation** — `CsvProductWriter` initially called `toPlainString()` instead of `toString()` on `BigDecimal`, which can produce different output for non-standard scales.
3. **Rounding order** — `ProductTransformer` initially rounded prices after all rules had run, meaning `PriceRangeRule` saw an unrounded price for non-Electronics products. Assignment 2 rounds all prices before both `CategoryUpgradeRule` and `PriceRangeRule`. The fix was to introduce `PriceRoundingRule` as an explicit second step in the rule sequence, ensuring the transformation order matched Assignment 2 exactly.

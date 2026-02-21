# AI Prompts — Assignment 3 Redesign Transcript

A complete record of the prompts used and responses received during the OO redesign of the
Assignment 2 ETL pipeline into Assignment 3.

---

## Prompt 1 — Responsibility Analysis

**Prompt:**
> Here is my Assignment 2 ETL pipeline code. Please analyze it and identify all the
> responsibilities currently handled in a single class or procedurally.

**Response summary:**
The AI identified eight distinct responsibilities collapsed into `ETLPipeline.main()`:

1. **Orchestration** — the `main()` method acts as glue, coordinating all steps
2. **File I/O (reading)** — `BufferedReader` setup and line iteration
3. **File I/O (writing)** — `BufferedWriter` setup and output header
4. **Input validation / row filtering** — blank lines, field count, `NumberFormatException`
5. **Data parsing / extraction** — converting raw CSV strings to typed values
6. **Business logic / transformation** — discount rule, rounding, category upgrade, price range
7. **Output formatting / serialisation** — reassembling fields into a CSV string
8. **Reporting / logging** — tracking row counts and printing the summary

---

## Prompt 2 — OO Decomposition Design

**Prompt:**
> Based on this code, suggest an object-oriented decomposition into multiple classes. Describe
> each class's responsibility, its fields, and its public methods. Do not code yet just respond.

**Response summary:**
The AI proposed seven classes plus a driver:

| Class | Responsibility |
|---|---|
| `Product` | Plain data object (model) holding typed product fields |
| `CsvProductReader` | Extract phase — file reading, parsing, validation |
| `ProductTransformer` | Transform phase — applies ordered list of rules |
| `PriceRangeClassifier` | Single-purpose label assignment from price |
| `CsvProductWriter` | Load phase — CSV serialisation and output |
| `PipelineReporter` | Statistics tracking and summary printing |
| `ETLPipeline` | Thin orchestrator — wires the components and calls `run()` |

Two interfaces were proposed (`ProductReader`, `ProductWriter`) to decouple the orchestrator from
CSV-specific implementations. Key design principles cited: Single Responsibility, Open/Closed,
Separation of Concerns, and Testability.

---

## Prompt 3 — Inheritance and Polymorphism Opportunities

**Prompt:**
> Which of these classes could use inheritance or polymorphism? Give me a concrete example using
> my actual code. Do not code yet just respond.

**Response summary:**
Three candidates were identified, each with a concrete tie to the original code:

1. **`CsvProductReader` / `CsvProductWriter` → interface polymorphism.** The hardcoded file
   paths on lines 23–26 of Assignment 2 mean the pipeline is locked to CSV. A `ProductReader`
   interface with `readAll()` allows a `DatabaseProductReader` or `JsonProductReader` to be
   substituted without changing `ETLPipeline`.

2. **`ProductTransformer` → Strategy pattern.** The three unrelated rules on lines 62–86
   (discount, category upgrade, price range) each become a `TransformationRule` implementation.
   `ProductTransformer` holds a `List<TransformationRule>` and calls `apply()` on each — adding
   or removing a rule requires no changes to the transformer.

3. **`ProductTransformer` → inheritance for category-specific logic.** The repeated
   `if (category.equals("Electronics"))` checks suggest subclasses (`ElectronicsTransformer`,
   `DefaultTransformer`) overriding an abstract `transform()` method.

The Strategy pattern on `ProductTransformer` was identified as providing the most immediate benefit.

---

## Prompt 4 — Class Diagram

**Prompt:**
> Create a class diagram in text form showing the relationships between the classes you proposed.

**Response summary:**
The AI produced a full ASCII UML class diagram showing:

- `ProductReader` and `ProductWriter` interfaces, each implemented by their CSV concrete classes
- `ETLPipeline` depending on all four collaborators via `uses` arrows
- `ProductTransformer` composing (`◆`) a `List<TransformationRule>`
- `TransformationRule` interface with three concrete implementations
- `PriceRangeRule` depending on `PriceRangeClassifier`
- `Product` as the data object flowing through reader → transformer → writer

A relationship key explained the symbols: `△` for implements, `◆` for composition, `▼ uses`
for dependency, and directional labels for data flow.

---

## Prompt 5 — Code Generation

**Prompt:**
> Now generate the Java code for each class in separate files under the package
> `org.howard.edu.lsp.assignment3` with full Javadocs for the class and all public methods.

**Response summary:**
The AI created 13 Java files under `src/org/howard/edu/lsp/assignment3/`:

`Product`, `ProductReader`, `CsvProductReader`, `ProductWriter`, `CsvProductWriter`,
`TransformationRule`, `ElectronicsDiscountRule`, `CategoryUpgradeRule`, `PriceRangeClassifier`,
`PriceRangeRule`, `ProductTransformer`, `PipelineReporter`, `ETLPipeline`

All classes included full class-level and method-level Javadocs. A design note was added: rounding
was placed inside `ElectronicsDiscountRule` so that `CategoryUpgradeRule` always compares against
the rounded price — matching the order of operations in the original Assignment 2.

---

## Prompt 6 — Output Parity Fix

**Prompt:**
> Make sure the main entry point class produces the exact same output as Assignment 2: same CSV
> format, same transformations, same error handling, and uses relative paths.

**Response summary:**
The AI performed a line-by-line audit and found four discrepancies:

| # | Location | A2 behaviour | A3 behaviour | Fix |
|---|---|---|---|---|
| 1 | `CsvProductReader` | `"found at "` | `"found: "` | Changed separator |
| 2 | `CsvProductWriter` | `price.toString()` | `price.toPlainString()` | Changed method |
| 3 | Rounding order | Rounds all prices before `PriceRange` assigned | `ProductTransformer` rounded after all rules | Added `PriceRoundingRule` |
| 4 | `ElectronicsDiscountRule` | Rounding is a separate step | Rule did its own rounding | Removed rounding from rule |

A new `PriceRoundingRule` class was created and inserted as step 2 in the rule sequence
(after discount, before category upgrade and price range), exactly mirroring line 68 of
Assignment 2. The stray final-round in `ProductTransformer.transform()` was removed.

---

## Prompt 7 — Test Plan

**Prompt:**
> How should I test that Assignment 3 produces identical output to Assignment 2? Give me a
> checklist and a test plan including the missing input file case and the empty input file case.

**Response summary:**
The AI produced a seven-case test plan:

| Test | Purpose |
|---|---|
| TC-01 Normal input | Golden path against real `products.csv` — verify CSV and console output |
| TC-02 Missing file | Exact error message: `ERROR: Input file not found at data/products.csv` |
| TC-03 Empty file | Zero bytes — header-only CSV, all counts 0 |
| TC-04 Header-only file | Header row but no data — same result as TC-03 |
| TC-05 All rows invalid | Verify skipped counts when every row fails validation |
| TC-06 Rounding boundary | Non-Electronics prices with > 2 dp verify `PriceRoundingRule` fires first |
| TC-07 Upgrade threshold | Electronics at exactly $500.00 should NOT upgrade; $500.40 should |

A comparison procedure was included: run both pipelines, `cp` each output CSV, then `diff` to
verify byte-for-byte equality. An error was also noted: the test plan initially stated "Rows read:
12, Rows skipped: 5" but the actual counts are 11 and 4 — the file ends with a single `\n` which
does not produce an extra `readLine()` call.

---

## Prompt 8 — Live Comparison Run

**Prompt:**
> Run the program and compare the output of Assignment 3 to Assignment 2's
> `transformed_products.csv`. Are there any differences?

**Response summary:**
Both pipelines were compiled and executed against the same input. Results:

- **Console output (both):** `Rows read: 11 / Rows transformed: 7 / Rows skipped: 4 / Output written to: data/transformed_products.csv`
- **`diff` result:** `FILES ARE IDENTICAL` — no differences found
- The test plan's incorrect row counts (12/5 vs actual 11/4) were explained: the trailing newline
  in the CSV file produces `null` from `readLine()`, not an empty string, so it is not counted.

---

## Prompt 9 — Test Commands

**Prompt:**
> Give me the commands to test the output of assignment 2 vs assignment 3.

**Response summary:**
The AI provided a complete set of copy-pasteable shell commands covering:

- Compiling both assignments with `javac -d bin`
- Running each with `java -cp bin` and saving the output with `cp`
- Diffing with `diff ... && echo "IDENTICAL"`
- Missing file test using `mv` to temporarily rename the input
- Empty file test using `> data/products.csv` to truncate and restore

---

## Prompt 10 — Javadoc Audit

**Prompt:**
> Review the Javadocs in all my Assignment 3 classes and flag any that are inaccurate, missing,
> or incomplete.

**Response summary:**
All 14 files were read and audited. Seven issues were found across five files:

| # | File | Issue type | Description |
|---|---|---|---|
| 1 | `Product.java:21` | Inaccurate | Field comment claimed name is "stored in uppercase" — not enforced by the class |
| 2 | `TransformationRule.java:12` | Incomplete | Ordering example skipped `PriceRoundingRule` between discount and category upgrade |
| 3 | `TransformationRule.java:16` | Missing | `@see PriceRoundingRule` absent from the `@see` list |
| 4 | `CategoryUpgradeRule.java:13` | Incomplete | Ordering precondition only mentioned `ElectronicsDiscountRule`, not `PriceRoundingRule` |
| 5 | `CategoryUpgradeRule.java:9` | Inaccurate | "post-discount price" should be "post-discount, rounded price" |
| 6 | `ETLPipeline.java:82` | Inaccurate | "halts without writing output" is wrong for write failures — file may be truncated |
| 7 | `PipelineReporter.java:62` | Incomplete | Increment methods are public API never called in the codebase — no guidance given |

---

## Prompt 11 — Javadoc Fixes

**Prompt:**
> Fix these javadoc mistakes.

**Response summary:**
All seven issues were fixed across five files:

1. `Product.java` — Removed `", stored in uppercase"` from field comment
2. `TransformationRule.java` — Updated ordering example to include `PriceRoundingRule`
3. `TransformationRule.java` — Added `@see PriceRoundingRule` to the `@see` block
4. `CategoryUpgradeRule.java` — Updated precondition to say "after both `ElectronicsDiscountRule` and `PriceRoundingRule`"
5. `CategoryUpgradeRule.java` — Changed "post-discount price" to "post-discount, rounded price" in both class and method doc
6. `ETLPipeline.java` — Split error doc into two sentences for read vs write failures
7. `PipelineReporter.java` — Added class-level explanation of setters vs incrementers; added cross-reference notes to each `increment*` method

---

## Prompt 12 — Reflection Document

**Prompt:**
> Write a 1–2 page REFLECTION.md comparing my Assignment 2 and Assignment 3 designs. Cover:
> what changed, how A3 is more OO, which OO concepts were used (object, class, encapsulation,
> inheritance, polymorphism), and how I tested that both produce the same output.

**Response summary:**
`REFLECTION.md` was written to `src/org/howard/edu/lsp/assignment3/doc/` covering four sections:

- **What changed** — single 100-line procedural method vs 14 classes in three layers, with a
  table mapping each layer to its classes
- **How A3 is more OO** — four prose paragraphs on objects and classes (`Product` as a named
  entity), encapsulation (each class hides its details), polymorphism (three interfaces), and
  inheritance (interface implementation as contract-based substitutability)
- **OO concepts summary table** — mapping each concept to its concrete location in the code
- **Testing section** — the exact shell commands, the console output produced by both pipelines,
  confirmation that `diff` reported no differences, and a callout of the three compatibility bugs
  caught during development

---

## Prompt 13 — This Document

**Prompt:**
> Create AI_PROMPTS.md formatted as a transcript of the prompts I used and your responses
> during this redesign.

**Response summary:**
This file.

# Development Log - Question 3

## External Resources Used

### AI Tool: Claude (Anthropic) - claude-sonnet-4-6

**What I asked / how it was used:**

I used Claude to help plan and implement the Strategy Pattern refactoring for the `PriceCalculator` class. Specifically, I asked it to:

1. Evaluate the original `PriceCalculator` design and identify maintenance and extensibility problems. This helped me organize my thinking for the `design_evaluation.md` write-up and articulate the Open/Closed Principle violation, the fragility of string-based dispatch, and the single responsibility issues clearly.

2. Generate a plan for refactoring the class using the Strategy Pattern, identifying what files to create (the `PricingStrategy` interface, four concrete strategy classes, the updated `PriceCalculator` context, and the `Driver`). This gave me a clear file-by-file breakdown before writing any code.

3. Implement each file with full Javadoc comments following the conventions already established in the project (author tag, method-level documentation). This sped up the boilerplate writing while letting me focus on verifying the logic was correct.

4. Write the `design_evaluation.md` document. I reviewed the draft and confirmed it accurately reflected the problems in the original class.

No other external resources (internet searches, textbooks, or lecture notes) were consulted beyond what was described above.

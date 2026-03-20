# Design Evaluation: PriceCalculator

## Overview

The original `PriceCalculator` class uses a series of `if` statements to apply different discounts based on a `String` customer type. While the code produces correct results, several design issues make it difficult to extend and maintain as the system grows.

---

## Problems with the Current Design

### 1. Violates the Open/Closed Principle

The class is not open for extension without modification. Every time a new customer type is introduced (for example, `"STUDENT"` or `"EMPLOYEE"`), the `calculatePrice` method must be edited. This risks introducing bugs in previously working discount logic and forces retesting of the entire method.

### 2. Chained Conditionals Instead of Polymorphism

The method uses four independent `if` checks rather than `else if` or a `switch`, which is logically fragile. More importantly, all four branches live in a single method, meaning the class is doing work that should be distributed across separate, interchangeable objects. This is a missed opportunity for polymorphism.

### 3. Single Responsibility Violation

`PriceCalculator` is responsible for knowing the discount rules of every customer type at once. If the `MEMBER` discount rate changes, this class must be modified even though nothing about `REGULAR`, `VIP`, or `HOLIDAY` has changed. Each discount rule should be its own responsibility.

### 4. String-Based Type Dispatch Is Fragile

Passing the customer type as a raw `String` provides no compile-time safety. A caller can pass `"member"` (lowercase) or `"Vip"` and receive the wrong result silently, since the method simply returns the original price with no error. A strategy object or enum would eliminate this class of bug entirely.

### 5. Not Extensible Without Recompilation

Because all logic is hard-coded inside one method, adding or removing a discount type requires recompiling `PriceCalculator` itself. In a larger system, this forces downstream classes that depend on `PriceCalculator` to be redeployed even if their behavior did not change.

---

## Conclusion

The core problem is that the design conflates selecting a discount algorithm with executing it. Separating these concerns through the Strategy Pattern allows each discount rule to be defined, tested, and modified independently, and keeps the `PriceCalculator` context class stable as new customer types are added.

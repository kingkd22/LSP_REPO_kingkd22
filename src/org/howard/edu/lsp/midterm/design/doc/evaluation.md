# Design Evaluation: OrderProcessor

## Overview

The `OrderProcessor` class exhibits several significant object-oriented design problems. The issues below reference principles from Arthur Riel's *Object-Oriented Design Heuristics* as well as general OOP concepts.

---

## Design Issues

### 1. Poor Encapsulation (Riel Heuristic 2.1)

All four data fields are declared `public`:

```java
public String customerName;
public String email;
public String item;
public double price;
```

This exposes internal state directly to any external code, eliminating any possibility of validation or controlled access. According to Riel, all data should be hidden within its class; no class should have public instance variables. Any code anywhere in the system can freely set `price` to a negative number or leave `email` null with no safeguard.

---

### 2. God Class / Single Method Does Everything (Riel Heuristic 3.1)

The single `processOrder()` method performs at least six unrelated operations:

1. Tax calculation
2. Receipt printing
3. File persistence (saving to `orders.txt`)
4. Email notification
5. Discount application
6. Activity logging

Riel warns against "god classes" — classes that do too much and know too much. A well-designed class should have a single, focused responsibility. `OrderProcessor` is doing the work of a tax engine, a printer, a file repository, an email service, and a logger all at once.

---

### 3. No Separation of Concerns

Business logic (tax computation, discount rules) is directly tangled with I/O operations (console printing, file writing, sending email). This makes it impossible to unit-test the tax or discount logic without triggering file writes and console output. It also means changing the storage format requires editing the same method that handles discounts.

---

### 4. Missing Domain Objects (Riel Heuristic 2.9)

There are no classes representing the core concepts of the domain. `customerName`, `email`, `item`, and `price` are just loose fields on a processor class. A proper design would have:

- A `Customer` class encapsulating name and email
- An `Order` or `OrderItem` class encapsulating item and price

Riel notes that modelling real-world entities as classes is a fundamental step in OO design. Lumping unrelated data into a single class because they happen to be used together is a procedural, not object-oriented, approach.

---

### 5. Hardcoded Magic Numbers

The tax rate (`0.07`), discount threshold (`500`), and discount multiplier (`0.9`) are all embedded as literal values:

```java
double tax = price * 0.07;
if(price > 500) {
    total = total * 0.9;
}
```

These should be named constants or configurable parameters. As written, a business rule change (e.g., a new tax rate) requires editing production logic rather than updating a configuration.

---

### 6. Logic Ordering Bug — Discount Applied After Receipt Is Printed

The receipt is printed before the discount is applied:

```java
// print receipt
System.out.println("Total: " + total);   // prints pre-discount total

// ...

// apply discount
if(price > 500) {
    total = total * 0.9;                 // discount applied too late
}
```

The customer's receipt will always show the wrong (higher) total for orders over $500. This bug exists precisely because all logic is crammed into a single sequential method with no clear structure. Each concern being isolated in its own class would make the correct ordering explicit.

---

### 7. Not Extensible (Violates Open/Closed Principle)

Switching to a database instead of a flat file, changing the email provider, or adding a new discount tier all require modifying `processOrder()` directly. The class is not open for extension without modification. A design that delegates each responsibility to a separate collaborating class allows components to be swapped or extended independently.

# Proposed Redesign: CRC Cards

The redesign distributes the responsibilities of the original `OrderProcessor` god class across eight focused classes. Each class owns a single concern, collaborating through well-defined interfaces.

---

## CRC Cards

---

**Class:** `Customer`

**Responsibilities:**
- Hold customer name and email address
- Provide read access to customer data

**Collaborators:**
- *(none)*

---

**Class:** `Order`

**Responsibilities:**
- Hold item name and base price
- Maintain a reference to the associated customer
- Store the computed total after tax and discounts are applied

**Collaborators:**
- `Customer`

---

**Class:** `TaxCalculator`

**Responsibilities:**
- Compute the tax amount for a given price
- Return the taxed total (price + tax)

**Collaborators:**
- *(none)*

---

**Class:** `DiscountService`

**Responsibilities:**
- Determine whether an order qualifies for a discount
- Apply the appropriate discount rate to the order total
- Return the discounted total

**Collaborators:**
- `Order`

---

**Class:** `ReceiptPrinter`

**Responsibilities:**
- Format a human-readable receipt for an order
- Print the receipt to the console

**Collaborators:**
- `Order`
- `Customer`

---

**Class:** `OrderRepository`

**Responsibilities:**
- Persist a completed order record to a file
- Manage the file I/O lifecycle (open, write, close)

**Collaborators:**
- `Order`
- `Customer`

---

**Class:** `EmailService`

**Responsibilities:**
- Send a confirmation email to the customer after an order is processed

**Collaborators:**
- `Customer`
- `Order`

---

**Class:** `OrderProcessor`

**Responsibilities:**
- Orchestrate the full order workflow in the correct sequence:
  1. Compute taxed total via `TaxCalculator`
  2. Apply discount via `DiscountService`
  3. Print receipt via `ReceiptPrinter`
  4. Persist order via `OrderRepository`
  5. Notify customer via `EmailService`
  6. Log completion

**Collaborators:**
- `Order`
- `TaxCalculator`
- `DiscountService`
- `ReceiptPrinter`
- `OrderRepository`
- `EmailService`

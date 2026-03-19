# Development Log

## External Resources Used

### AI Tool: Claude (Anthropic) — claude-sonnet-4-6


**Prompt:**

Evaluating Object-Oriented Design. The following class is part of a simple order processing system. The design of this class violates several object-oriented design ideas discussed in class, including principles described in Arthur Riel's object-oriented design heuristics. Study the code carefully and answer the questions that follow.

Provided Class:

```java
import java.io.FileWriter;
import java.util.Date;

public class OrderProcessor {

    // Order data (poor encapsulation)
    public String customerName;
    public String email;
    public String item;
    public double price;

    public void processOrder() {

        // calculate tax
        double tax = price * 0.07;
        double total = price + tax;

        // print receipt
        System.out.println("Customer: " + customerName);
        System.out.println("Item: " + item);
        System.out.println("Total: " + total);

        // save order to file
        try {
            FileWriter writer = new FileWriter("orders.txt", true);
            writer.write(customerName + "," + item + "," + total + "\n");
            writer.close();
        } catch(Exception e) {
            e.printStackTrace();
        }

        // send confirmation email
        System.out.println("Sending confirmation email to " + email);

        // apply discount
        if(price > 500) {
            total = total * 0.9;
        }

        // log activity
        System.out.println("Order processed at " + new Date());
    }
}
```

Part 1 - Design Evaluation

Evaluate the design of the OrderProcessor class. Based on the object-oriented design principles discussed in class, describe the design issues you observe in this class. Explain why the design may lead to poor object-oriented structure, maintainability, or extensibility. You may reference ideas from Arthur Riel's design heuristics, but exact terminology is not required.
Your answer must be written in a Markdown file named: evaluation.md
Place the file in the directory: org/howard/edu/lsp/midterm/design/doc

Part 2 - Proposed Improved Design Using CRC Cards

Redesign the system to improve its object-oriented structure. Express your proposed design using CRC cards (Class–Responsibility–Collaborator). Your CRC cards should identify the major components of the redesigned system and how responsibilities are distributed among them.
Your answer must be written in a Markdown file named: design.md
Place the file in the directory: org/howard/edu/lsp/midterm/design/doc
Each CRC card should follow this format:
Class: Responsibilities: Collaborators:

Part 3 - Development Log

Create a Markdown file named development_log_q2.md and place it in: org/howard/edu/lsp/midterm/design/doc

This file must document any external resources used while completing this question, including AI tools, internet searches, documentation, lecture notes, or textbooks. For each resource used include what you searched or asked and how the information helped you complete the problem. If no external assistance was used, clearly state that in the file. Do the same as the other development_log.md and use a prompt that would get me the same output as im receiving now

---

**Response produced:** All three documentation files were created under `src/org/howard/edu/lsp/midterm/design/doc/`:

- `evaluation.md` — identifies seven distinct design issues in the `OrderProcessor` class: poor encapsulation (public fields), god class / single method doing everything, no separation of concerns, missing domain objects (`Customer`, `Order`), hardcoded magic numbers, a logic ordering bug where the discount is applied after the receipt is printed, and lack of extensibility (violates Open/Closed principle). Each issue references relevant Riel heuristics or OOP principles.

- `design.md` — proposes a redesign using eight CRC cards: `Customer`, `Order`, `TaxCalculator`, `DiscountService`, `ReceiptPrinter`, `OrderRepository`, `EmailService`, and `OrderProcessor`. Each card lists focused responsibilities and collaborators, distributing the god class's duties across purpose-built classes.

- `development_log_q2.md` — this file; documents the AI tool and full prompt used.

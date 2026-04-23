# Question 5 Answers

## Heuristic 1:

**Name:**
All data should be hidden within its class.

**Explanation:**
This heuristic says that instance variables should be private and only accessible through well-defined methods. It improves maintainability because you can change the internal representation of a class without affecting any code that uses it. In lecture, this was illustrated with the example of making fields public versus private, and how a public field locks you into a specific internal type because every caller now depends on it directly. The point was that once you expose data, you lose the ability to change it without breaking things outside the class.

---

## Heuristic 2:

**Name:**
Do not create God classes.

**Explanation:**
A God class is a class that does too much and knows too much about the rest of the system. This heuristic says that system intelligence should be spread across classes in a way that matches the problem domain, rather than piling behavior into one central class. It improves readability because smaller, focused classes are easier to understand in isolation. In lecture, this came up when we talked about single responsibility and how a class that handles data storage, business logic, and output formatting all at once is a warning sign. The discussion tied directly into why design patterns like Strategy exist, because they distribute behavior into separate classes instead of branching everything inside one.

---

## Heuristic 3:

**Name:**
The interface of a class should provide a consistent level of abstraction.

**Explanation:**
This heuristic says that all the public methods on a class should operate at roughly the same level of detail. If some methods are high-level ("processOrder") and others are low-level ("incrementCounter"), the class is probably doing work it should not be doing or exposing implementation details that should be private. It improves readability because callers can look at the public interface and get a clear sense of what the class is for without having to figure out which methods are meant for external use and which are really internal helpers. In lecture, this came up in the context of Question 1-style problems, specifically whether a method like `getNextId()` belongs in the public interface at all, since it is a low-level implementation detail that exists only to serve `addRequest()`.

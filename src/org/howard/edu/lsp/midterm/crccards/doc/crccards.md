# CRC Cards Reflection

TaskManager collaborates with Task because its responsibilities require it to directly operate on Task objects: storing them, finding them by ID, and filtering them by status. Task does not collaborate with TaskManager because its own responsibilities (storing task information, updating its status, and providing its details) are fully self-contained and require no knowledge of the collection that holds it. This one-directional dependency keeps Task independent and reusable, since a managed object does not need to know about its manager.

# DB-Xtract

Welcome to the db-Xtract!

DB-Xtract is an attempt to create a scalable, fault-tolerant and flexible change data capture (CDC) tool. A this very first moment, it is providing journal-based CDC, but I want to add other mechanisms for more performance and lower resources consumption.

DB-Xtract can be distributed as concurrent CDC agents, providing scalability, availability and performance. It relies on Apache's ZooKeeper to coordinate data captures in a safe and still fast way.

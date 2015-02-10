# Minibase
Database Implementation Project for Miniature Database, Minibase in Java
Minibase is a miniature relational DBMS.
This version of Minibase makes use of Java's built-in runtime exceptions. In general, only two exceptions are necessary:
• IllegalArgumentException: Throw this when a method argument is invalid, for example if the user attempts to select a record that doesn't exist.
• IllegalStateException: Throw this if the database reaches an undefined state, for example if the user attempts to get the next record after completing a scan.

# resurfaceio-continuity-checker
Continuity checker for Resurface database

This command-line utility verifies that the count of calls in the database monotonically increases over time.
Reports shard & count query times in addition to detecting any reduction in the number of available calls.

```
java -cp "lib/*" src/Main.java
```

---
<small>&copy; 2016-2023 <a href="https://resurface.io">Resurface Labs Inc.</a></small>

# Distributed Database

A small distributed database with concurrency control, deadlock avoidance, fault tolerance and
failure recovery.

A very brief overview of the algorithms used:

1. Available copies algorithm for data replication to enhance fault tolerance.

2. Two phase locking using shared (read) and exclusive (write) locks for read-write transactions. Read-only transactions use multiversion read consistency, where the transaction obtains no locks but reads only the committed values of data items at the time the transaction began.

3. Avoided deadlocks using the wait-die protocol, in which an older transaction waits for a younger one that holds a conflicting lock on the data-item of interest, but a younger transaction will abort instead of waiting for an older one.

4. Failure-recovery: Unreplicated data is available immediately for reading on a recovered site. Replicated data items on a recovered site are available for writing, but not for reading until a committed write has taken place on the data item at the recovered site. This is to enforce consistency, as the locks held by transactions on a site can be lost when the site fails.

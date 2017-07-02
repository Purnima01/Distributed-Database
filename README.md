# Distributed Database

A small distributed database with concurrency control (2PL), deadlock avoidance (Wait-die), fault tolerance (available copies) and failure recovery (available copies).

*http://cs.nyu.edu/courses/fall15/CSCI-GA.2434-001/handDB2.pdf

*http://cs.nyu.edu/courses/fall16/CSCI-GA.2434-001/transproc.pdf

A brief overview of the algorithms used:

1. Available copies algorithm for data replication, to enhance fault tolerance. If a site is down, replication allows data items to be read from other sites that host the data item.

2. Two phase locking using shared (read) and exclusive (write) locks for read-write transactions. Read-only transactions use multiversion read consistency (MVRC), where the transaction obtains no locks but reads only the committed values of data items at the time the transaction began. MVRC is advantageous in the following ways:
  * RO-txns do not obtain read locks, thereby avoiding subsequent RW-txns from being blocked on these RO-txns.
  * An existing txn holding an exclusive lock on the data item of interest does not block a RO-txn from executing, since the       RO-txn will read only the committed value of the data item.

3. Avoided deadlocks using the wait-die protocol, in which an older transaction waits for a younger one that holds a conflicting lock on the data item of interest, but a younger transaction will abort instead of waiting for an older one. Wait-die results in a lot of aborts, because many conflicts end in abort. It's reasonable when conflicts are rare and the developer doesn't  want to go to the trouble of implementing cycle detection. Older transactions get priority because a transaction that aborts and restarts gets its original start time, so eventually it will finish.

4. Failure-recovery: (part of available copies) Unreplicated data is available immediately for reading on a recovered site. Replicated data items on a recovered site are available for writing, but not for reading until a committed write has taken place on the data item at the recovered site. This is to enforce consistency, so that the db at the recovered site doesn't return stale information, in case of any updates to the data item at other sites while the site was down.

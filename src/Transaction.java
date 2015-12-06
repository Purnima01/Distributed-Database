import java.util.*;

/**
 * Represents transactions: Regular(RW) and Read-Only(RO)
 */
public class Transaction {
    private final int startTime;
    private String id;
    private TransactionStatus status;

    //Site where lock is held and list of locks held by txn on that site
    private Map<Integer, List<Lock>> locksHeldByTxn;
    private Set<Integer> sitesAccessed;
     /*
      * modifiedVariables:
      * Local cache for written-to variables;
      * String = variable name, Integer = value written by this txn
      */
    private Map<String, Integer> modifiedVariables;
    private final TransactionType type;

    public Transaction(int beginTime, String txnId, TransactionType txnType) {
        modifiedVariables = new HashMap<String, Integer>();
        startTime = beginTime;
        id = txnId;
        status = TransactionStatus.ACTIVE;
        type = txnType;
        sitesAccessed = new HashSet<Integer>();
        if (type == TransactionType.REGULAR) {
            locksHeldByTxn = new HashMap<Integer, List<Lock>>();
        }
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public int getStartTime() {
        return startTime;
    }

    public String getId() {
        return id;
    }

    public TransactionType getType() {
        return type;
    }

    /**
     * For deadlock avoidance using wait-die, this informs us if
     * the current transaction is younger than the other transaction.
     * T2 is younger than T1 if T2's startTime > T1's starttime.
     * @param other transaction to compare age to
     * @return True if current transaction is younger than the
     * other transaction. False otherwise.
     */
    public boolean isYoungerThan(Transaction other) {
        int myStartTime = startTime;
        int otherStartTime = other.getStartTime();
        if (myStartTime > otherStartTime) {
            return true;
        }
        return false;
    }

    public void abort(Site[] sites, String reasonForAbort) {
        status = TransactionStatus.ABORTED;

        removeSelfFromAccessedSites(sites);

        System.out.println(reasonForAbort);

        releaseAllLocksHeld(sites);

        reclaimSpace();
    }

    private void releaseAllLocksHeld(Site[] sites) {
        if (type == TransactionType.REGULAR) {
            Set<Integer> sitesAccessed = locksHeldByTxn.keySet();
            for (Integer site : sitesAccessed) {
                List<Lock> lockList = locksHeldByTxn.get(site);
                for (Lock lock : lockList) {
                    lock.release(sites);
                }
            }
        }
    }

    private void removeSelfFromAccessedSites(Site[] sites) {
        for (Integer accessedSiteId : sitesAccessed) {
            Site siteAccessed = sites[accessedSiteId];
            siteAccessed.removeTransaction(id);
        }
    }

    private void reclaimSpace() {
        modifiedVariables = null;
        locksHeldByTxn = null;
        sitesAccessed = null;
    }

    /**
     * Commits and sends any modified variables back to the tm. Warning: sends
     * the actual object. Any changes made to the object will be reflected in the
     * actual data structure in the transaction.
     */
    public Map<String, Integer> commitAndPushChanges(Site[] sites) {
        removeSelfFromAccessedSites(sites);
        releaseAllLocksHeld(sites);
        status = TransactionStatus.COMMITTED;
        System.out.println("Transaction " + id + " has committed");
        return modifiedVariables;
    }

    public boolean variablePresentInModifiedVariables(String variable) {
        return (modifiedVariables.containsKey(variable));
    }

    public void addSiteToTxn(int siteid) {
        sitesAccessed.add(siteid);
    }

    public void addLockInformationToTransaction(Lock lock) {
        int siteOnWhichLockIsHeld = lock.getSiteIdOnWhichLockExists();
        List<Lock> locksOnSite;

        if (locksHeldByTxn.containsKey(siteOnWhichLockIsHeld)) {
            locksOnSite = locksHeldByTxn.get(siteOnWhichLockIsHeld);
        } else {
            locksOnSite = new ArrayList<Lock>();
        }
        locksOnSite.add(lock);
        locksHeldByTxn.put(siteOnWhichLockIsHeld, locksOnSite);
    }

    public boolean alreadyHasLockOnSiteForVariable(int siteId, String var) {
        if (!locksHeldByTxn.containsKey(siteId)) {
            return false;
        }
        List<Lock> locks = locksHeldByTxn.get(siteId);
        for (Lock lock : locks) {
            if (lock.getVariableLocked().equals(var)) {
                return true;
            }
        }
        return false;
    }

    /**Reads value from local storage*/
    public void readValueFromModifiedVariables(String varToAccess) {
        System.out.println("Value of " + varToAccess +  " read by "
                + getId() + " is " + modifiedVariables.get(varToAccess));
    }

    public void writeToLocalValue(String varToAccess, int valToWrite) {
        addToModifiedVariables(varToAccess, valToWrite);
    }

    /**
     * TM calls this when a transaction wants to write
     * to a variable and variable is not already in
     * modifiedVariables.
     * Adds a variable to this modifiedVariables iff:
     * 1. Transaction wants to write to the variable
     * 2. Transaction SUCCESSFULLY obtained the lock
     * on this variable. Does not add if the transaction
     * is currently waiting on this variable.
     *
     * All reads/writes for this txn must first check for
     * variables in modifiedVariables.
     *
     * @param varToAccess variable to be added.
     * @param valToWrite the value of variable in permanent
     * storage on the TM.
     */
    public void addToModifiedVariables(String varToAccess, int valToWrite) {
        modifiedVariables.put(varToAccess, valToWrite);
        System.out.println("Transaction " + getId() + " wrote " +
                varToAccess + " = " + valToWrite + " to local storage");
    }
}


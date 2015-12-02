import java.util.*;

/**
 * Represents transactions: Regular(RW) and Read-Only(RO)
 */
public class Transaction {
    private final int startTime;
    private String id;
    private TransactionStatus status;

    //integer corresponds to site where lock is held
    private Map<Integer, List<Lock>> locksHeldByTxn;
    private Set<Integer> sitesAccessed;
    private Map<String, Integer> modifiedVariables;
    /*
     * modifiedVariables:
     * Local cache for written-to variables;
     * String = variable name, Integer = value written by txn
     */
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

    public void setStatus(TransactionStatus newStatus) {
        status = newStatus;
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

        for (Integer accessedSiteId : sitesAccessed) {
            Site siteAccessed = sites[accessedSiteId];
            siteAccessed.removeTransaction(id);
        }

        System.out.println("Transaction " + id + " has been aborted because " + reasonForAbort);

        if (type == TransactionType.REGULAR) {
            Set<Integer> sitesAccessed = locksHeldByTxn.keySet();
            for (Integer site : sitesAccessed) {
                List<Lock> lockList = locksHeldByTxn.get(site);
                for (Lock lock : lockList) {
                    lock.release(sites);
                }
            }
        }
        reclaimSpace();
    }

    private void reclaimSpace() {
        modifiedVariables = null;
        locksHeldByTxn = null;
        sitesAccessed = null;
    }

    public void commit() {
        /*propagate my modified variables to tm -
          all my writes should be visible now
          and flush my modified variables*/
        //remove myself from every site's transactionsOnSite that I accessed
        //release all locks(unmodifiedVariablesAccessed + modifiedVariables)
        //on variables held by transaction
        //set my status to committed
        //set status as committed in tm for me
        //print that I am committed
    }

    public void waitTxn(){}

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
     * @param variable variable to be added.
     * @param currentValue the value of variable in permanent
     * storage on the TM.
     */
    public void addVariableToWriteVariables(String variable, int currentValue) {
        modifiedVariables.put(variable, currentValue);
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

    /**Reads value from local storage if exists*/
    public void readValueFromModifiedVariables(String varToAccess) {
        System.out.println("Value of " + varToAccess +  " read by "
                + getId() + " is " + modifiedVariables.get(varToAccess));
    }
}


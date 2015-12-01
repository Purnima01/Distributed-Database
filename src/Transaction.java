import java.util.*;

/**
 * Represents transactions: Regular(RW) and Read-Only(RO)
 */
public class Transaction {
    private final int startTime;
    private String id;
    private TransactionStatus status;

    private Set<Integer> sitesAccessed;
    private Map<String, Integer> modifiedVariables;
    /*
     * modifiedVariables:
     * Local cache for written-to variables;
     * String = variable name, Integer = value written by txn
     * unmodifiedVariablesAccessed:
     * Set of all the read variables accessed -
     * needed in case of abortion/commit, to release read locks
     */
    private List<String> unmodifiedVariablesAccessed;
    private final TransactionType type;

    public Transaction(int beginTime, String txnId, TransactionType txnType) {
        modifiedVariables = new HashMap<String, Integer>();
        unmodifiedVariablesAccessed = new ArrayList<String>();
        startTime = beginTime;
        id = txnId;
        status = TransactionStatus.ACTIVE;
        type = txnType;
        sitesAccessed = new HashSet<Integer>();
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
            //if non-RO release all locks (unmodifiedVariablesAccessed + modifiedVariables) on variables held by transaction
        }
        //flush my modified variables to claim space - set to null
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

    /**
     * TM calls this when a txn wants to read a variable:
     * 1. The variable is not present in modifiedVariables
     * 2. This txn has obtained the lock on the variable
     */
    public void addVariableToReadVariables(String variable) {
        unmodifiedVariablesAccessed.add(variable);
    }

    public boolean variablePresentInModifiedVariables(String variable) {
        return (modifiedVariables.containsKey(variable));
    }

    public void addSiteToTxn(int siteid) {
        sitesAccessed.add(siteid);
    }
}


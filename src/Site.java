import java.util.*;

/**
 * Denotes the database at a particular site
 */
public class Site {
    private int id;
    private SiteStatus siteStatus;
    //Variables on site and whether that variable can be read from this site
    private Map<String, Boolean> variablesOnSite;
    //Transactions that accessed any data item on this site
    private Set<String> transactionsOnSite;
    //String corresponds to variable that is locked on site
    private Map<String, List<Lock>> lockMap;

    public Site(int siteID) {
        siteStatus = SiteStatus.ACTIVE;
        id = siteID;
        variablesOnSite = new HashMap<String, Boolean>();
        transactionsOnSite = new HashSet<String>();
        lockMap = new HashMap<String, List<Lock>>();
    }

    public boolean allEvenVariablesWrittenToAfterRecovery() {
        Set<String> allVariablesOnSite = variablesOnSite.keySet();
        for (String variable : allVariablesOnSite) {
            if (!variablesOnSite.get(variable)) {
                return false;
            }
        }
        return true;
    }

    public SiteStatus getSiteStatus() {
        return siteStatus;
    }

    public void setSiteStatus(SiteStatus newStatus) {
        siteStatus = newStatus;
    }

    public int getId() {
        return id;
    }

    public Map<String, Boolean> getVariablesOnSite() {
        return variablesOnSite;
    }

    /**For recovered sites*/
    public void alterReadPermissionForVariable(String variable) {
        variablesOnSite.put(variable, true);
    }

    public void addVariableToSite(String variable) {
        variablesOnSite.put(variable, true);
    }

    public void addTxnToSite(String txnid) {
        transactionsOnSite.add(txnid);
    }

    public boolean canReadVariable(String varToAccess) {
        return variablesOnSite.get(varToAccess);
    }

    public Set<String> getTransactionsOnSite() {
        return transactionsOnSite;
    }

    public void clearTransactionsOnSite() {
        transactionsOnSite.clear();
    }

    public void removeTransaction(String txid) {
        transactionsOnSite.remove(txid);
    }

    public List<Lock> getLocksForVariable(String var) {
        return lockMap.get(var);
    }

    public void addToLockMap(Lock newReadLock) {
        String variable = newReadLock.getVariableLocked();
        List<Lock> locksForVariable;

        if (!lockMap.containsKey(variable)) {
            locksForVariable = new ArrayList<Lock>();
        } else {
            locksForVariable = lockMap.get(variable);
        }
        locksForVariable.add(newReadLock);
        lockMap.put(variable, locksForVariable);
    }

    /**
     * Get list of all locks on the variable.
     * Desired lock will be on this list.
     * Remove desired lock from list.
     *
     * This operation is O(N) per call,
     * where N = number of locks on
     * to this data item by all non-RO
     * transactions accessing this site.
     */
    public void removeLockEntry(Lock lockToRemove) {
        String variableCorrespondingToLock = lockToRemove.getVariableLocked();
        List<Lock> getLockList = lockMap.get(variableCorrespondingToLock);
        getLockList.remove(lockToRemove);
    }
    //deb:
    public void printLock(String v) {
        List<Lock> ll = lockMap.get(v);
        for (Lock l : ll) {
            System.out.println("Lock held by trans " + l.getTxnIdHoldingLock());
        }
    }
}

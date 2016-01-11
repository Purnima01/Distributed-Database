import java.util.*;

/**
 * Denotes the database at a particular site
 * Even-indexed data items are replicated at
 * every site. Odd-indexed data items are found
 * at sites (1 + (index % 10))
 * Note: 'variable' and 'data item' used interchangeably
 */
public class Site {
    private int id;
    private SiteStatus siteStatus;
    //Variables on this site and permission to read the variable on this site
    private Map<String, Boolean> variablesOnSite;
    //Variable and value-time history of the variable
    private Map<String, List<ValueTimeStamp>> variableValues;
    //Transactions that accessed any variable on this site
    private Set<String> transactionsOnSite;
    //Variable and list of locks on this variable at this site
    private Map<String, List<Lock>> lockMap;
    //<TxnID, <Variable (temporarily) modified by the txn, Value of the variable written by txn>>
    private Map<String, Map<String, Integer>> localStorage;

    public Site(int siteID) {
        localStorage = new HashMap<String, Map<String, Integer>>();
        siteStatus = SiteStatus.ACTIVE;
        id = siteID;
        variablesOnSite = new HashMap<String, Boolean>();
        transactionsOnSite = new HashSet<String>();
        lockMap = new HashMap<String, List<Lock>>();
        variableValues = new HashMap<String, List<ValueTimeStamp>>();
    }

    /**
     * If all replicated variables have been written
     * to after site-recovery, the status of the site can
     * be changed back to ACTIVE
     */
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

    public void addVariableToSite(String variable, ValueTimeStamp valTime) {
        variablesOnSite.put(variable, true);
        List<ValueTimeStamp> values = new ArrayList<ValueTimeStamp>();
        values.add(valTime);
        variableValues.put(variable, values);
    }

    public void updateValueOfVariable(String variable, ValueTimeStamp updatedValueTime) {
        List<ValueTimeStamp> history = variableValues.get(variable);
        history.add(updatedValueTime);
    }

    public List<ValueTimeStamp> getValueHistoryOfVariable(String variable) {
        return variableValues.get(variable);
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

    /** Add lock to the list of locks for the variable to be locked */
    public void addToLockMap(Lock newLock) {
        String variable = newLock.getVariableLocked();
        List<Lock> locksForVariable;

        if (!lockMap.containsKey(variable)) {
            locksForVariable = new ArrayList<Lock>();
        } else {
            locksForVariable = lockMap.get(variable);
        }
        locksForVariable.add(newLock);
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

    public void printSpecificVariableValue(String var) {
        List<ValueTimeStamp> history = variableValues.get(var);
        ValueTimeStamp latestValTs = history.get(history.size() - 1);
        int val = latestValTs.getValue();
        System.out.println("Variable " + var + " has value: " + val + " at site " + getId());
    }

    public void printVariableValuesOnSite() {
        Set<String> allVars = variableValues.keySet();
        Set<String> orderedVars = new TreeSet<String>(new MyComp());
        orderedVars.addAll(allVars);
        for (String var : orderedVars) {
            printSpecificVariableValue(var);
        }
    }

    public boolean presentInLocalStorage(String txnID, String varToAccess) {
        if (!localStorage.containsKey(txnID)) {
            return false;
        }
        Map<String, Integer> variableValueLocalMap = localStorage.get(txnID);
        if (!variableValueLocalMap.containsKey(varToAccess)) {
            return false;
        }
        return true;
    }

    public int getFromLocalStorage(String txnID, String varToAccess) {
        return ((localStorage.get(txnID)).get(varToAccess));
    }

    public void addToLocalStorage(String txnID, String varToAccess, int value) {
        Map<String, Integer> correspondingVariableMapForTxn;
        if (!localStorage.containsKey(txnID)) {
            correspondingVariableMapForTxn = new HashMap<String, Integer>();
        } else {
            correspondingVariableMapForTxn = localStorage.get(txnID);
        }
        correspondingVariableMapForTxn.put(varToAccess, value);
        localStorage.put(txnID, correspondingVariableMapForTxn);
        /*System.out.println("Transaction " + txnID + " wrote " + varToAccess +
            " = " + value + " to scratch-pad on site " + id);*/
    }

    public Map<String,Integer> getVariablesModified(String txnID) {
        return localStorage.get(txnID);
    }

    public void clearLocalStorage() {
        localStorage.clear();
    }

    public void removeFromLocalStorage(String txnId) {
        localStorage.remove(txnId);
    }

    private class MyComp implements Comparator<String> {
        public int compare(String s1, String s2) {
            String s1num = s1.substring(1);
            String s2num = s2.substring(1);
            int num1 = Integer.parseInt(s1num);
            int num2 = Integer.parseInt(s2num);
            return (num1 < num2 ? -1 : 1);
        }
    }
}

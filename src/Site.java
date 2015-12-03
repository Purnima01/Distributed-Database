import java.util.*;

/**
 * Denotes the database at a particular site
 */
public class Site {
    private int id;
    private SiteStatus siteStatus;
    //Variables on site and whether that variable can be read from this site
    private Map<String, Boolean> variablesOnSite;
    private Map<String, List<ValueTimeStamp>> variableValues;
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
        variableValues = new HashMap<String, List<ValueTimeStamp>>();
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

    public void printSpecificVariableValue(String var) {
        List<ValueTimeStamp> history = variableValues.get(var);
        ValueTimeStamp latestValTs = history.get(history.size() - 1);
        int val = latestValTs.getValue();
        System.out.println("Variable " + var + " has value: " + val + " on site " + getId());
    }

    public void printVariableValuesOnSite() {
        Set<String> allVars = variableValues.keySet();
        Set<String> orderedVars = new TreeSet<String>(new MyComp());
        orderedVars.addAll(allVars);
        for (String var : orderedVars) {
            printSpecificVariableValue(var);
        }
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

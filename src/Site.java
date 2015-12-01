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
    public Site(int siteID) {
        siteStatus = SiteStatus.ACTIVE;
        id = siteID;
        variablesOnSite = new HashMap<String, Boolean>();
        transactionsOnSite = new HashSet<String>();
    }

    //some ds for variable, txn holding lock on var, lock details

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
}

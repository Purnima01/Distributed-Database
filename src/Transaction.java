import java.util.*;

/**
 * Represents transactions: Regular(RW) and Read-Only(RO)
 * and other relevant properties
 */
public class Transaction {
    private final int startTime;
    private String id;
    private TransactionStatus status;

    //Site where lock is held and list of locks held by txn on that site
    private Map<Integer, List<Lock>> locksHeldByTxn;
    private Set<Integer> sitesAccessed;

    private final TransactionType type;

    public Transaction(int beginTime, String txnId, TransactionType txnType) {
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
        performActionAndCleanUp(TransactionStatus.ABORTED, reasonForAbort, sites);
    }

    public void commit(Site[] sites) {
        if (status == TransactionStatus.ABORTED) {
            return;
        }
        String printMsg = "Transaction " + id + " has committed";
        performActionAndCleanUp(TransactionStatus.COMMITTED, printMsg, sites);
    }

    private void performActionAndCleanUp(TransactionStatus status, String printMsg,
                                        Site[] sites) {
        this.status = status;
        removeSelfFromAccessedSites(sites);
        System.out.println(printMsg);
        releaseAllLocksHeld(sites);
        reclaimSpace();
    }

    private void releaseAllLocksHeld(Site[] sites) {
        if (type == TransactionType.REGULAR) {
            if (locksHeldByTxn == null) {
                return;
            }
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
        if (sitesAccessed == null) {
            return;
        }
        for (Integer accessedSiteId : sitesAccessed) {
            Site siteAccessed = sites[accessedSiteId];
            siteAccessed.removeFromLocalStorage(id);
            siteAccessed.removeTransaction(id);
        }
    }

    private void reclaimSpace() {
        locksHeldByTxn = null;
        sitesAccessed = null;
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

    public Set<Integer> getSitesAccessed() {
        return sitesAccessed;
    }
}


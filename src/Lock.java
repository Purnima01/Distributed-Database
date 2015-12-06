/**
 * Lock details:
 * data item that is locked by this lock
 * transaction holding this lock
 * type of lock (read/write)
 * site on which this lock is held
 */
public class Lock {
    private String txnIdHoldingLock;
    private int siteIdOnWhichLockExists;
    private String variableLocked;
    private LockType typeOfLock;

    /**
     * @param txnID id of the transaction holding this lock
     * @param siteID id of the site on which this lock is held
     * @param var data item that is locked
     * @param locktype write or read lock
     */
    public Lock(String txnID, int siteID, String var, LockType locktype) {
        txnIdHoldingLock = txnID;
        siteIdOnWhichLockExists = siteID;
        variableLocked = var;
        typeOfLock = locktype;
    }

    public String getTxnIdHoldingLock() {
        return txnIdHoldingLock;
    }

    public int getSiteIdOnWhichLockExists() {
        return siteIdOnWhichLockExists;
    }

    public LockType getTypeOfLock() {
        return typeOfLock;
    }

    public String getVariableLocked() {
        return variableLocked;
    }

    /**
     * Transaction calls this on abort/commit.
     * Releases this lock on the data item held by
     * the transaction on a particular site.
     */
    public void release(Site[] sites) {
        int siteId = getSiteIdOnWhichLockExists();
        Site site = sites[siteId];
        site.removeLockEntry(this);
    }
}

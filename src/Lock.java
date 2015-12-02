/**
 * Lock details:
 * data item that is locked by this lock
 * transaction holding the lock
 * type of lock (read/write)
 * site the lock is held on
 * lock id
 */
public class Lock {
    private static int lockid = 0;
    private String txnIdHoldingLock;
    private int siteIdOnWhichLockExists;
    private String variableLocked;
    private LockType typeOfLock;

    public Lock(String txnID, int siteID, String var, LockType locktype) {
        lockid ++;
        txnIdHoldingLock = txnID;
        siteIdOnWhichLockExists = siteID;
        variableLocked = var;
        typeOfLock = locktype;
    }

    public int getLockid() {
        return lockid;
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

    public void setTypeOfLock(LockType typeOfLock) {
        this.typeOfLock = typeOfLock;
    }

    /**
     * change existing read lock to write lock for variable, site and txn
     * when txn wants to write to a var on site it previously read.
     */
    public void updateLockChangeLockType(Lock lock, LockType changedType) {
        //fill during writes
    }

    /**Transaction calls this on abort/commit*/
    public void release(Site[] sites) {
        int siteId = getSiteIdOnWhichLockExists();
        Site site = sites[siteId];
        site.removeLockEntry(this);
    }
}

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
}

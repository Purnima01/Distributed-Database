/**
 * Represents a command for a particular transaction.
 * Eg: R(T2, x5).
 */

//todo: add dump stuff constructors later
public class Command {
    private Operation operation;
    private String txn;
    private String var;
    //for write commands:
    private Integer toWriteValue;
    //for fail/recover:
    private int siteAffected;
    private boolean inPendingList = false;
    private DumpType dumpType;
    private int dumpValue;

    /**
     * use this for fail/recover cmds
     */
    public Command(Operation op, int siteHit) {
        operation = op;
        siteAffected = siteHit;
    }

    /**
     * Use this for read/write cmds
     */
    public Command(Operation op, String t, String v) {
        operation = op;
        txn = t;
        var = v;
        toWriteValue = null;
    }

    /**
     * Use this for begin/beginRO/end cmds
     */
    public Command(Operation op, String transaction) {
        operation = op;
        txn = transaction;
    }

    /**
     * Use this for dump cmds
     * @param op dump
     * @param dumpType dump specific site, all sites or specific variable at all sites
     * @param dumpValue which dump site/variable to dump
     */
    public Command(Operation op, DumpType dumpType, int dumpValue) {
        operation = op;
        this.dumpType = dumpType;
        this.dumpValue = dumpValue;
    }

    public boolean isInPendingList() {
        return inPendingList;
    }

    public void setInPendingList(boolean val) {
        inPendingList = val;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getTransaction() {
        return txn;
    }

    public String getVar() {
        return var;
    }

    public int getSiteAffected() {
        return siteAffected;
    }

    public int getToWriteValue() {
        return toWriteValue;
    }

    public DumpType getDumpType() {
        return dumpType;
    }

    public int getDumpValue() {
        return dumpValue;
    }
    /**
     * Use this for write commands. Eg: W(T1, x3, 10);
     * @param value: value to be written. In the above eg., 10.
     */
    public void setToWriteValue(int value) {
        toWriteValue = value;
    }

}

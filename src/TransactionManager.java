
import java.io.FileNotFoundException;
import java.util.*;

/**
 * This class is responsible for coordinating the
 * activities of all the transactions on the distributed
 * database. It oversees the management of all the sites
 * and overall management of the database.
 */
public class TransactionManager {
    static final int SITES = 11;
    private int time = 0;

    private Map<String, Transaction> transactionMap;
    //Variables and the sites they are present on
    private Map<String, List<Site>> variableLocationMap;
    private Map<String, List<ValueTimeStamp>> variableMap;

    //10 sites, ignoring site[0]
    private Site[] sites;

    private ArrayList<Command> pendingCommands;

    public TransactionManager() {
        variableLocationMap = new HashMap<String, List<Site>>();
        variableMap = new HashMap<String, List<ValueTimeStamp>>();
        transactionMap = new HashMap<String, Transaction>();
        sites = new Site[SITES];
        pendingCommands = new ArrayList<Command>();
    }

    private void initialize() {
        initializeSites();
        initializeVariables();
        distributeVariablesToSites();
    }

    private void initializeSites() {
        for (int i = 1; i <= 10; i ++) {
            sites[i] = new Site(i);
        }
    }

    private void initializeVariables() {
        for (int i = 1; i <= 20; i ++) {
            String variable = getVariableName(i);
            String value = "10" + String.valueOf(i);
            int initVal = Integer.parseInt(value);
            int initTime = 0;
            ValueTimeStamp varVal = new ValueTimeStamp(initVal, initTime);
            List<ValueTimeStamp> valTimeList = new ArrayList<ValueTimeStamp>();
            valTimeList.add(varVal);
            variableMap.put(variable, valTimeList);
        }
    }

    /**
     * Odd-indexed variables are at one site each (non-replicated)
     * Even-indexed variables are at all sites (replicated)
     */

    private void distributeVariablesToSites() {
        for (int var = 1; var <= 20; var ++) {
            String varName = getVariableName(var);
            if (var % 2 == 0) {
                for (Site site : sites) {
                    //skip 0th site because there is no site there
                    if (site == null) {
                        continue;
                    }
                    List<Site> locations;
                    site.addVariableToSite(varName);
                    if (variableLocationMap.containsKey(varName)) {
                        locations = variableLocationMap.get(varName);
                    } else {
                        locations = new ArrayList<Site>();
                    }
                    locations.add(site);
                    variableLocationMap.put(varName, locations);
                }
            } else {
                int siteIndex = 1 + (var % 10);
                Site siteLocatedAt = sites[siteIndex];
                List<Site> location = new ArrayList<Site>();
                location.add(siteLocatedAt);
                variableLocationMap.put(varName, location);
                siteLocatedAt.addVariableToSite(varName);
            }
        }
    }

    private String getVariableName(int varIdx) {
        String variableName = "x" + String.valueOf(varIdx);
        return variableName;
    }

    public int getTime() {
        return time;
    }

    public void incrementTime() {
        time ++;
    }

    private void putCommandInPendingListIfAbsent(Command cmd) {
        if (!cmd.isInPendingList()) {
            cmd.setInPendingList(true);
            pendingCommands.add(cmd);
        }
    }

    private void removeCommandFromPendingListIfPresent(Command cmd) {
        if (cmd.isInPendingList()) {
            cmd.setInPendingList(false);
            pendingCommands.remove(cmd);
        }
    }

    private void updateSiteAndTransactionRecords(Site serveSite, Transaction txn) {
        serveSite.addTxnToSite(txn.getId());
        txn.addSiteToTxn(serveSite.getId());
    }

    public static void main(String[] args) throws FileNotFoundException {
        TransactionManager tm = new TransactionManager();
        ReadFileInput rf = new ReadFileInput("/Users/purnima/Desktop/adbms/Project/tests");

        tm.initialize();

        while (rf.hasNextLine()) {
            tm.incrementTime();
            List<Command> cmdsForLine = rf.getLineAsCommands();

            /*todo: for each command, do corresponding task
            once all the commands in line have been processed, check if any pending txns can be processed now*/
            for (Command cmd : cmdsForLine) {
                switch (cmd.getOperation()) {

                    case BEGIN:
                        String txnID = cmd.getTransaction();
                        Transaction txn = new Transaction(tm.getTime(),
                                txnID, TransactionType.REGULAR);
                        tm.transactionMap.put(txnID, txn);
                        break;

                    case BEGINRO:
                        txnID = cmd.getTransaction();
                        txn = new Transaction(tm.getTime(),
                                txnID, TransactionType.READONLY);
                        tm.transactionMap.put(txnID, txn);
                        break;

                    case END:
                        txnID = cmd.getTransaction();
                        Transaction txnAboutToCommit = tm.transactionMap.get(txnID);
                        txnAboutToCommit.commit();
                        break;

                    case READ:
                        txnID = cmd.getTransaction();
                        txn = tm.transactionMap.get(txnID);
                        String varToAccess = cmd.getVar();

                        if (txn.getType() == TransactionType.READONLY) {
                            tm.processROtxn(txn, varToAccess, cmd);
                        } else {
                            tm.processRWtxn(txn, varToAccess, cmd);
                        }
                        break;

                    case RECOVER:
                        int siteNumberToRecover = cmd.getSiteAffected();
                        tm.processRecovery(siteNumberToRecover);
                        break;

                    case FAIL:
                        int siteNumberToFail = cmd.getSiteAffected();
                        tm.processFail(siteNumberToFail);
                        break;

                    case WRITE:
                        //for recovered sites, some form of check if the variables are all written to so it can be
                        //active again instaad of recovered
                        break;
                    //deal with pending list in FIFO order
                }
            }
        }
        System.out.println("Number of pending commands = " + tm.pendingCommands.size());
    }

    private void processRecovery(int siteNumberToRecover) {
        Site siteToRecover = sites[siteNumberToRecover];
        siteToRecover.setSiteStatus(SiteStatus.RECOVERED);
        Map<String, Boolean> variablesOnSite = siteToRecover.getVariablesOnSite();
        List<String> blockReadsToVariableList = new ArrayList<String>();

        for(Map.Entry<String, Boolean> entry : variablesOnSite.entrySet()) {
            String variable = entry.getKey();
            if (isEven(variable)) {
                blockReadsToVariableList.add(variable);
            }
        }

        for (String variable : blockReadsToVariableList) {
            variablesOnSite.put(variable, false);
        }

        siteToRecover.clearTransactionsOnSite();
    }

    private boolean isEven(String variable) {
        //0th character is 'x' in 'x20'
        int varNum = Integer.parseInt(variable.substring(1));
        if ((varNum % 2) == 0) {
            return true;
        }
        return false;
    }

    private void processFail(int siteNumberToFail) {
        Site siteToFail = sites[siteNumberToFail];
        siteToFail.setSiteStatus(SiteStatus.FAILED);
        Set<String> allTransactionsOnSite = siteToFail.getTransactionsOnSite();
        List<Transaction> abortTxnListForSite = new ArrayList<Transaction>();

        for (String transID : allTransactionsOnSite) {
            Transaction correspondingTransaction = transactionMap.get(transID);
            abortTxnListForSite.add(correspondingTransaction);
        }

        for (Transaction transaction : abortTxnListForSite) {
            String reasonForAbort = ("site " + siteNumberToFail + " has failed");
            transaction.abort(sites, reasonForAbort);
        }
    }

    /*Used by RW txns only*/
    private void printVariableValueRead(String varToAccess,
          Transaction txn, Site serveSite) {

        List<ValueTimeStamp> valueHistoryForVariable = variableMap.get(varToAccess);
        int size = valueHistoryForVariable.size();
        int valueOfVariable = valueHistoryForVariable.get(size - 1).getValue();
        System.out.println("Value of " + varToAccess +  " read by "
                + txn.getId() + " is " + valueOfVariable
                + " at site " + serveSite.getId());
    }

    private boolean canRunTxn(Transaction txn) {
        if (txn.getStatus() == TransactionStatus.COMMITTED) {
            return false;
        }
        if (txn.getStatus() == TransactionStatus.ABORTED) {
            return false;
        }
        return true;
    }

    private Site findSiteThatCanServeRequestedVariable(String varToAccess, Transaction txn) {
        List<Site> sitesWithVariable = variableLocationMap.get(varToAccess);
        Site serveSite = null;
        for (Site site : sitesWithVariable) {
            if (site.getSiteStatus() == SiteStatus.FAILED) {
                continue;
            }
            if (site.getSiteStatus() == SiteStatus.RECOVERED &&
                    !site.canReadVariable(varToAccess)) {
                System.out.println("Transaction " + txn.getId() +
                        " cannot read variable " + varToAccess +
                        " on site " + site.getId() +
                        " because the site was recovered and the" +
                        " replicated data item is yet to be written to.");
                continue;
            }
            serveSite = site;
            break;
        }
        return serveSite;
    }

    private boolean existsWriteLockOnVariableByAnotherTransaction(Command cmd,
           String varToAccess, Transaction currentTxn, Site serveSite) {

        List<Lock> locksOnVariableOnServeSite = serveSite.getLocksForVariable(varToAccess);
        //no locks for this variable on site yet
        if (locksOnVariableOnServeSite == null) {
            return false;
        }

        for (Lock lock : locksOnVariableOnServeSite) {
            if (lock.getTypeOfLock() == LockType.WRITELOCK) {
                String txnIdHoldingLock = lock.getTxnIdHoldingLock();
                Transaction txnHoldingLock = transactionMap.get(txnIdHoldingLock);
                if (currentTxn.isYoungerThan(txnHoldingLock)) {
                    String reasonForAbort = ("Transaction " + currentTxn.getId() +
                            " was aborted (wait-die) because it was waiting on a lock" +
                            " held by Transaction " + txnHoldingLock.getId());
                    currentTxn.abort(sites, reasonForAbort);
                } else {
                    //currentTxn is older than owner of lock; so it must wait for owner to complete
                    pendingCommands.add(cmd);
                }
                return true;
            }
        }
        return false;
    }

    private void addReadLock(String varToAccess, Site serveSite, Transaction currentTxn) {
        Lock newReadLock = new Lock(currentTxn.getId(),
                serveSite.getId(), varToAccess, LockType.READLOCK);
        serveSite.addToLockMap(newReadLock);

        currentTxn.addLockInformationToTransaction(newReadLock);
        updateSiteAndTransactionRecords(serveSite, currentTxn);
        printVariableValueRead(varToAccess, currentTxn, serveSite);
    }

    private void processRWtxn(Transaction txn, String varToAccess, Command cmd) {
        if (!canRunTxn(txn)) {
            return;
        }

        if (txn.variablePresentInModifiedVariables(varToAccess)) {
            txn.readValueFromModifiedVariables(varToAccess);
            return;
        }

        Site serveSite = findSiteThatCanServeRequestedVariable(varToAccess, txn);
        //txn has to wait till site becomes available - add cmd to pendinglist
        if (serveSite == null) {
            putCommandInPendingListIfAbsent(cmd);
            return;
        }

        if (txn.alreadyHasLockOnSiteForVariable(serveSite.getId(), varToAccess)) {
            System.out.println("Transaction " + txn.getId() + " already has a lock" +
                    " on variable " + varToAccess + " at site " + serveSite.getId());
            printVariableValueRead(varToAccess, txn, serveSite);
            return;
        }

        Transaction currentTxn = txn;

        if (existsWriteLockOnVariableByAnotherTransaction(cmd, varToAccess,
                currentTxn, serveSite)) {
            return;
        }

        //no write lock, safe to add a read lock to the var on site
        addReadLock(varToAccess, serveSite, currentTxn);
    }

    private void processROtxn(Transaction txn, String varToAccess, Command cmd) {
        if (!canRunTxn(txn)) {
            return;
        }

        int startTimeTxn = txn.getStartTime();
        Site serveSite = findSiteThatCanServeRequestedVariable(varToAccess, txn);
        //txn has to wait till site becomes available - add cmd to pendinglist
        if (serveSite == null) {
            putCommandInPendingListIfAbsent(cmd);
            return;
        } else {
            //site is available - if cmd was in pendinglist, remove it
            removeCommandFromPendingListIfPresent(cmd);

            //in case of site failure
            updateSiteAndTransactionRecords(serveSite, txn);

            List<ValueTimeStamp> valueHistoryForVariable =
                    variableMap.get(varToAccess);

            int index = 0;
            while (index < valueHistoryForVariable.size()) {
                if (valueHistoryForVariable.get(index).getTime() <= startTimeTxn) {
                    index ++;
                }
            }
            index --;

            printVariableValueReadByROTransaction(index, varToAccess,
                    valueHistoryForVariable, txn, serveSite);
        }
    }

    private void printVariableValueReadByROTransaction(int index,
            String varToAccess,
            List<ValueTimeStamp> valueHistoryForVariable,
            Transaction txn, Site serveSite) {

        int valueOfVariableReadByROTxn = valueHistoryForVariable.get(index).getValue();
        System.out.println("Value of " + varToAccess + " read by "
                + txn.getId() + " is " + valueOfVariableReadByROTxn
                + " at site " + serveSite.getId());
    }
}

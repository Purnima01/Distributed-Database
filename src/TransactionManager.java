//write part pending + full testing

//TODO: Check debs and todos
//Remember to skip 0th site for sites
//what happens if a site fails - suppose t1 acc. x4 on site1
//and t2 acc. x4 on site2 and site1 fails, do we kill t1 & t2?
//what about for write txns?

/*TODO: remove comments that are not needed
* TODO: clean up addREadLock addWriteLock commin part
* TODO: mention in javadocs whenever objects are returned from getters and not their copies.
* TODO: changes propagate to the actual object.
* TODO: change "variable" in print statements to "data item"
* TODO: somehow merge sites accessed set and lock info (also has sites accessed as key in Reg txns)..duplicate ds for same infor!
*/

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

    //10 sites, ignoring site[0]
    private Site[] sites;

    private List<Command> pendingCommands;

    private List<Command> commandsToRemoveFromPendingListForThisRound;

    public TransactionManager() {
        variableLocationMap = new HashMap<String, List<Site>>();
        transactionMap = new HashMap<String, Transaction>();
        sites = new Site[SITES];
        pendingCommands = new ArrayList<Command>();
        commandsToRemoveFromPendingListForThisRound = new ArrayList<Command>();
    }

    private void initialize() {
        initializeSites();
        distributeVariablesToSites();
    }

    private void initializeSites() {
        for (int i = 1; i <= 10; i ++) {
            sites[i] = new Site(i);
        }
    }

    private ValueTimeStamp initializeVariable(int variable) {
            String value = "10" + String.valueOf(variable);
            int initVal = Integer.parseInt(value);
            int initTime = 0;
            ValueTimeStamp varVal = new ValueTimeStamp(initVal, initTime);
            return varVal;
    }

    /**
     * Odd-indexed variables are at one site each (non-replicated)
     * Even-indexed variables are at all sites (replicated)
     */

    private void distributeVariablesToSites() {
        for (int var = 1; var <= 20; var ++) {
            String varName = getVariableName(var);
            ValueTimeStamp variableValue = initializeVariable(var);
            if (var % 2 == 0) {
                for (Site site : sites) {
                    //skip 0th site because there is no site there
                    if (site == null) {
                        continue;
                    }
                    List<Site> locations;
                    site.addVariableToSite(varName, variableValue);
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
                siteLocatedAt.addVariableToSite(varName, variableValue);
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
            commandsToRemoveFromPendingListForThisRound.add(cmd);
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

            for (Command cmd : cmdsForLine) {
                tm.executeCommand(cmd);
            }
            //Traverse pending list to see if any command can be processed after executing recent commands
            tm.executeCommandsInPendingList();
        }
        System.out.println("Number of pending commands = " + tm.pendingCommands.size());
    }

    private void executeCommandsInPendingList() {
        commandsToRemoveFromPendingListForThisRound.clear();
        for (Command pending : pendingCommands) {
            executeCommand(pending);
        }
        for (Command command : commandsToRemoveFromPendingListForThisRound) {
            pendingCommands.remove(command);
        }
    }

    private void executeCommand(Command cmd) {
        switch (cmd.getOperation()) {

            case BEGIN:
                String txnID = cmd.getTransaction();
                Transaction txn = new Transaction(getTime(),
                        txnID, TransactionType.REGULAR);
                transactionMap.put(txnID, txn);
                break;

            case BEGINRO:
                txnID = cmd.getTransaction();
                txn = new Transaction(getTime(),
                        txnID, TransactionType.READONLY);
                transactionMap.put(txnID, txn);
                break;

            case READ:
                txnID = cmd.getTransaction();
                txn = transactionMap.get(txnID);
                String varToAccess = cmd.getVar();

                if (txn.getType() == TransactionType.READONLY) {
                    processROtxn(txn, varToAccess, cmd);
                } else {
                    processRWtxn(txn, varToAccess, cmd);
                }
                break;

            case WRITE:
                txnID = cmd.getTransaction();
                txn = transactionMap.get(txnID);
                varToAccess = cmd.getVar();
                int valueToWrite = cmd.getToWriteValue();

                processWrite(txn, varToAccess, valueToWrite, cmd);
                break;

            case RECOVER:
                int siteNumberToRecover = cmd.getSiteAffected();
                processRecovery(siteNumberToRecover);
                break;

            case FAIL:
                int siteNumberToFail = cmd.getSiteAffected();
                processFail(siteNumberToFail);
                break;

            case END:
                txnID = cmd.getTransaction();
                Transaction txnAboutToCommit = transactionMap.get(txnID);
                signalCommitAndReceiveChanges(txnAboutToCommit);
                break;

            case DUMP:
                if (cmd.getDumpType() == DumpType.NONE) {
                    dumpAllSites();
                } else if (cmd.getDumpType() == DumpType.SITE) {
                    int siteToDump = cmd.getDumpValue();
                    dumpSpecificSite(siteToDump);
                } else {
                    int varToDump = cmd.getDumpValue();
                    dumpVariable(varToDump);
                }
                break;
        }
    }

    private void dumpVariable(int varToDump) {
        String reconstructVariable = "x" + String.valueOf(varToDump);
        List<Site> sitesWithVar = variableLocationMap.get(reconstructVariable);
        for (Site site : sitesWithVar) {
            site.printSpecificVariableValue(reconstructVariable);
        }
    }

    private void dumpAllSites() {
        for (Site site : sites) {
            if (site != null) {
                dumpSiteHelp(site);
            }
        }
    }

    private void dumpSpecificSite(int siteId) {
        Site site = sites[siteId];
        dumpSiteHelp(site);
    }

    /**Prints all variable values on the site*/
    private void dumpSiteHelp(Site site) {
        System.out.println("Variables on site " + site.getId());
        site.printVariableValuesOnSite();
    }

    /**call this on encountering end cmd*/
    public void signalCommitAndReceiveChanges(Transaction txn) {
        Map<String, Integer> modifiedVariables = txn.commitAndPushChanges(sites);
        //for each changed variable, propagate change to all sites
        Set<String> variablesChanged = modifiedVariables.keySet();
        //txn performed no writes
        if (variablesChanged.size() == 0) {
            return;
        }
        for (String variable : variablesChanged) {
            int newValue = modifiedVariables.get(variable);
            updateGlobalValueOfVariable(variable, newValue);
        }
    }

    /**
     * Call when a write transaction commits - all the variables written to
     * by the write transaction are now sent to every site that holds the variable.
     * A value-timestamp entry is created for the variable and is appended
     * to the corresponding list of value-timestamp history for the variable
     * in variableValues of the site. This list will not include a failed site as
     * the transaction would have been aborted if a site it had written to had failed.
     */
    public void updateGlobalValueOfVariable(String variableToUpdate, int newValue) {
        List<Site> sitesWithVariable = variableLocationMap.get(variableToUpdate);
        propagateUpdatedVariableToRelevantSites(variableToUpdate, newValue, sitesWithVariable);
    }

    private void propagateUpdatedVariableToRelevantSites(String variable,
                 int newValue, List<Site> sitesWithVariable) {

        ValueTimeStamp update = new ValueTimeStamp(newValue, time);
        for (Site site : sitesWithVariable) {
            site.updateValueOfVariable(variable, update);
            if (site.getSiteStatus() == SiteStatus.RECOVERED) {
                site.alterReadPermissionForVariable(variable);
            }
            if (site.allEvenVariablesWrittenToAfterRecovery()) {
                site.setSiteStatus(SiteStatus.ACTIVE);
            }
        }
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

        List<ValueTimeStamp> valueHistoryForVariable =
                serveSite.getValueHistoryOfVariable(varToAccess);
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
                    putCommandInPendingListIfAbsent(cmd);
                }
                return true;
            }
        }
        return false;
    }

    private void addReadLock(String varToAccess, Site serveSite, Transaction currentTxn) {
        addLock(varToAccess, serveSite, currentTxn, LockType.READLOCK);
        printVariableValueRead(varToAccess, currentTxn, serveSite);
    }

    private void addLock(String varToAccess, Site site, Transaction txn, LockType lockType) {
        Lock newLock = new Lock(txn.getId(), site.getId(), varToAccess, lockType);
        site.addToLockMap(newLock);

        txn.addLockInformationToTransaction(newLock);
        updateSiteAndTransactionRecords(site, txn);
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
        removeCommandFromPendingListIfPresent(cmd);
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
            //in case of site failure
            updateSiteAndTransactionRecords(serveSite, txn);

            List<ValueTimeStamp> valueHistoryForVariable =
                    serveSite.getValueHistoryOfVariable(varToAccess);

            int index = 0;
            while (index < valueHistoryForVariable.size()) {
                if (valueHistoryForVariable.get(index).getTime() < startTimeTxn) {
                    index ++;
                }
            }
            index --;

            printVariableValueReadByROTransaction(index, varToAccess,
                    valueHistoryForVariable, txn, serveSite);

            removeCommandFromPendingListIfPresent(cmd);
        }
    }

    private void processWrite(Transaction txn, String varToAccess, int valToWrite, Command cmd) {
        if (!canRunTxn(txn)) {
            return;
        }

        if (txn.variablePresentInModifiedVariables(varToAccess)) {
            txn.writeToLocalValue(varToAccess, valToWrite);
            return;
        }
        List<Lock> existingReadLocksForTxn = new ArrayList<Lock>();
        WriteOperationStatus result = attemptToWrite(varToAccess,
                txn, cmd, existingReadLocksForTxn);
        if (result == WriteOperationStatus.ABORTED) {
            return;
        } else if (result == WriteOperationStatus.WAIT) {
            return;
        } else {
            executeWrite(varToAccess, valToWrite, existingReadLocksForTxn, txn);
            removeCommandFromPendingListIfPresent(cmd);
        }
    }

    private void executeWrite(String varToAccess, int valToWrite,
            List<Lock> existingReadLocksForTxn, Transaction txn) {
        List<Site> sitesWithVariable = variableLocationMap.get(varToAccess);
        txn.addToModifiedVariables(varToAccess, valToWrite);

        for (Site site : sitesWithVariable) {
            if (site.getSiteStatus() == SiteStatus.FAILED) {
                continue;
            }
            //add write lock on every active site that has variable
            addLock(varToAccess, site, txn, LockType.WRITELOCK);
            //remove all read locks held by this txn on this variable at any site (from prv step)
            for (Lock lock : existingReadLocksForTxn) {
                lock.release(sites);
            }
        }
    }

    private WriteOperationStatus attemptToWrite(
            String varToAccess, Transaction currentTxn, Command cmd,
            List<Lock> existingReadLocks) {

        if (noActiveSite(varToAccess)) {
            putCommandInPendingListIfAbsent(cmd);
            return WriteOperationStatus.WAIT;
        }

        List<Site> sitesWithVar = variableLocationMap.get(varToAccess);
        for (Site site : sitesWithVar) {
            if (site.getSiteStatus() == SiteStatus.FAILED) {
                continue;
            }
            List<Lock> locksOnVar = site.getLocksForVariable(varToAccess);
            //no existing lock on variable on this site
            if (locksOnVar == null) {
                continue;
            }

            for (Lock lock : locksOnVar) {
                String otherTxnId = lock.getTxnIdHoldingLock();
                Transaction otherTxn = transactionMap.get(otherTxnId);
                /*
                  existing read lock on variable and site by the currentTxn - ok
                  but needs to be removed after acquiring write-locks
                 */
                if (otherTxn.getId().equals(currentTxn.getId())) {
                    existingReadLocks.add(lock);
                    continue;
                }
                if (currentTxn.isYoungerThan(otherTxn)) {
                    String reasonForAbort = ("Transaction " + currentTxn.getId() +
                            " was aborted (wait-die) because it was waiting on a lock" +
                            " held by Transaction " + otherTxn.getId());
                    currentTxn.abort(sites, reasonForAbort);
                    return WriteOperationStatus.ABORTED;
                } else {
                    /*
                    currentTxn is older than otherTxn holding lock, must wait.
                    Note: this cmd is added to pendingList. On a later site, this
                    txn might abort as it might encounter an older txn with lock on
                    variable. Then, it still remains in pendingList. However, this
                    is ok, as when we process pendingList cmds, this transaction will
                    be aborted and that command for the txn will be ignored.
                     */
                    putCommandInPendingListIfAbsent(cmd);
                    return WriteOperationStatus.WAIT;
                }
            }
        }
        return WriteOperationStatus.WRITE;
    }

    private boolean noActiveSite(String variable) {
        List<Site> sites = variableLocationMap.get(variable);
        for (Site site : sites) {
            if (site.getSiteStatus() != SiteStatus.FAILED) {
                return false;
            }
        }
        return true;
    }
}

//TODO: Check debs and possopts
//Remember to skip 0th site for sites
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
/**
 * This class is responsible for coordinating the
 * activities of all the transactions on the distributed
 * database. It oversees the management of all the sites
 * and overall management of the database.
 */
public class TransactionManager {
    static final int SITES = 11;
    static final int VARIABLES = 21;
    private int time = 0;

    private Map<Transaction, TransactionStatus> transactionMap;
    //Variables and the sites they are present on
    private Map<String, List<Site>> variableLocationMap;
    private Map<String, List<ValueTimeStamp>> variableMap;

    //10 sites, ignoring site[0]
    private Site[] sites;

    private ArrayList<Command> pendingCommands;

    public TransactionManager() {
        variableLocationMap = new HashMap<String, List<Site>>();
        variableMap = new HashMap<String, List<ValueTimeStamp>>();
        transactionMap = new HashMap<Transaction, TransactionStatus>();
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

    public static void main(String[] args) throws FileNotFoundException {
        TransactionManager tm = new TransactionManager();
        ReadFileInput rf = new ReadFileInput("/Users/purnima/Desktop/adbms/Project/tests");

        tm.initialize();

        /*while (rf.hasNextLine()) {
            tm.incrementTime();
            List<Command> cmdsForLine = rf.getLineAsCommands();
            System.out.println("At time = " + tm.getTime() + ": ");
            for (Command c : cmdsForLine) {
                System.out.print(c.getOperation() + " ");
            }
            System.out.println();
        }*/
    }
}

import java.util.*;

/**
 * Denotes the database at a particular site
 */
public class Site {
    private int id;
    private SiteStatus siteStatus;
    private Set<String> variablesOnSite;
    //String below corresponds to variable that is locked
    private Map<String, List<Lock>> lockMap;
    public Site(int siteID) {
        siteStatus = SiteStatus.ACTIVE;
        id = siteID;
        variablesOnSite = new HashSet<String>();
        lockMap = new HashMap<String, List<Lock>>();
    }

    public int getId() {
        return id;
    }

    public Set<String> getVariablesOnSite() {
        return variablesOnSite;
    }

    public void addVariableToSite(String variable) {
        variablesOnSite.add(variable);
    }

}

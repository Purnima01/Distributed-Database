/**
 * For dump(...) commands
 * Used to denote if it is a:
 * dump() : print all variable values at all sites
 * dump(i) : print variable values at site i
 * dump(xj) : print variable xj values at all sites
 */
public enum DumpType {
    SITE, VARIABLE, NONE;
}

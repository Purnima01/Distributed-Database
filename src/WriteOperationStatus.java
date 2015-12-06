/**
 * Status of a write command when a
 * RW transaction performs a write -
 * denotes if the write cmd is successful,
 * is on waitlist for an event to occur
 * (lock release, site recovery)
 * or unsuccessful
 */
public enum WriteOperationStatus {
    ABORTED, WAIT, WRITE;
}

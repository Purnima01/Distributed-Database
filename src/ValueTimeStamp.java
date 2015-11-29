/**
 * Class to represent the value of a variable at a given time.
 */
public class ValueTimeStamp {
    private int value;
    private int time;
    public ValueTimeStamp(int val, int t) {
        value = val;
        time = t;
    }

    public int getValue() {
        return value;
    }

    public int getTime() {
        return time;
    }
}

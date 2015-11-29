import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Class to handle input of commands read in from file.
 * Stores the command in a Command object that it returns
 * to the TransactionManager.
 */
public class ReadFileInput {

    private Scanner scan;

    /**
     * Constructor for the class.
     * @param  filePath the location of the
     * file containing the input
     */
    public ReadFileInput(String filePath) throws FileNotFoundException {
        scan = new Scanner(new File(filePath));
    }

    public boolean hasNextLine() {
        return scan.hasNextLine();
    }

    public List<Command> getLineAsCommands() {
        while (scan.hasNextLine()) {
            String line = scan.nextLine();
            List<Command> commandsForCurrentLine =
                    parseCurrentLineIntoCommands(line);
            return commandsForCurrentLine;
        }
        return null;
    }

    /**
     * returns the co-temporous events on a line as
     * individual commands to the TM in the following steps:
     * 1. break up line into co-temporous command strings
     * 2. remove all whitespaces in each command string
     * 3. find which constructor this command should invoke
     *      - for begin, first check if it matches beginRO
     * 4. call the constructor with the appropriate values
     *      - for writes, also set write value
     * 5. accumulate the commands in a List and return this
     */
    private List<Command> parseCurrentLineIntoCommands(String line) {
        String[] commandStrings = line.split("; ");
        List<Command> commandsOnLine = new ArrayList<Command>();
        for (String cmd : commandStrings) {
            cmd = cmd.replaceAll("\\s+","");
            //find which command cmd corresponds to
            switch (cmd.charAt(0)) {
                case 'b':
                    Command command;
                    if (cmd.contains("RO")) {
                        String txn = cmd.substring(8, cmd.length() - 1);
                        command = new Command(Operation.BEGINRO, txn);
                    } else {
                        String txn = cmd.substring(6, cmd.length() - 1);
                        command = new Command(Operation.BEGIN, txn);
                    }
                    commandsOnLine.add(command);
                    break;

                case 'e':
                    String txn = cmd.substring(4, cmd.length() - 1);
                    command = new Command(Operation.END, txn);
                    commandsOnLine.add(command);
                    break;

                case 'f':
                    String siteToFail = cmd.substring(5, cmd.length() - 1);
                    command = new Command(Operation.FAIL, Integer.parseInt(siteToFail));
                    commandsOnLine.add(command);
                    break;

                case 'r':
                    String siteToRecover = cmd.substring(8, cmd.length() - 1);
                    command = new Command(Operation.RECOVER, Integer.parseInt(siteToRecover));
                    commandsOnLine.add(command);
                    break;

                case 'R':
                    String information = cmd.substring(2, cmd.length() - 1);
                    String[] parts = information.split(",");
                    txn = parts[0];
                    String var = parts[1];
                    command = new Command(Operation.READ, txn, var);
                    commandsOnLine.add(command);
                    break;

                case 'W':
                    information = cmd.substring(2, cmd.length() - 1);
                    parts = information.split(",");
                    txn = parts[0];
                    var = parts[1];
                    String val = parts[2];
                    command = new Command(Operation.WRITE, txn, var);
                    command.setToWriteValue(Integer.parseInt(val));
                    commandsOnLine.add(command);
                    break;

                case 'd':
                    command = new Command(Operation.DUMP);
                    commandsOnLine.add(command);
                    break;
            }
        }
        return commandsOnLine;
    }


}

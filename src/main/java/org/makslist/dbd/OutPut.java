package org.makslist.dbd;

import java.util.*;

public class OutPut {

    private static final Scanner SCANNER = new Scanner(System.in);
    private static OutPut output;

    public static OutPut getInstance(Level level) {
        if (OutPut.output == null) {
            OutPut.output = new OutPut(level);
        }
        return OutPut.output;
    }

    public static OutPut getInstance() {
        return OutPut.output != null ? OutPut.output : OutPut.getInstance(Level.USER);
    }

    private final Level level;

    private OutPut(Level level) {
        this.level = level;
    }

    public void user(String message) {
        System.out.print(message);
    }

    public void userln(String message) {
        System.out.println(message);
    }

    public boolean question(String message, String yesOption, String noOption) {
        System.out.print(message + " [" + yesOption + "/" + noOption + "]");
        return yesOption.equalsIgnoreCase(SCANNER.nextLine());
    }

    public void error(String message) {
        if (level.ordinal() >= Level.ERROR.ordinal()) System.err.println(message);
    }

    public void info(String message) {
        if (level.ordinal() >= Level.INFO.ordinal()) System.out.println(message);
    }

    public void debug(String message) {
        if (level.ordinal() >= Level.DEBUG.ordinal()) System.out.println(message);
    }

    public enum Level {
        SILENT, ERROR, QUESTION, USER, INFO, DEBUG
    }

}
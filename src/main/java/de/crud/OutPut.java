package de.crud;

public class OutPut {

    private static OutPut output;

    public static OutPut create(Level level) {
        if (OutPut.output == null) {
            OutPut.output = new OutPut(level);
        }
        return OutPut.output;
    }

    public static OutPut getInstance() {
        return OutPut.output != null ? OutPut.output : OutPut.create(Level.USER);
    }

    private final Level level;

    private OutPut(Level level) {
        this.level = level;
    }

    public void user(String message) {
        System.out.println(message);
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
        USER, ERROR, SILENT, INFO, DEBUG
    }

}
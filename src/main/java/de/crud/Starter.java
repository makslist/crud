package de.crud;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;

public class Starter {

    private static final String LOGO = "  _____  ____                 _ _\n" +
            " |  __ \\|  _ \\     /\\        | | |\n" +
            " | |  | | |_) |   /  \\    ___| | |_ __ _\n" +
            " | |  | |  _ <   / /\\ \\  / _ \\ | __/ _` |\n" +
            " | |__| | |_) | / /__\\ \\|  __/ | || (_| |\n" +
            " |_____/|____/ /________\\\\___|_|\\__\\__,_|\n";

    public static final String FILE_EXTENSION = "snapshot";

    private static final SimpleDateFormat EXPORT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmm");

    public static void main(String[] args) {
        Config config = Config.parseArgs(args);

        OutPut output;
        if (config.isVerbose()) output = OutPut.getInstance(OutPut.Level.INFO);
        else output = OutPut.getInstance(OutPut.Level.USER);

        if (config.isHelp()) {
            output.userln(LOGO);
            output.userln(Config.COMMAND_LINE_PARAMETER);
            System.exit(0);
        }

        output.userln("DBΔelta");

        if (config.getVendor() == null) {
            output.error("\nNo vendor given!");
            output.error(Config.COMMAND_LINE_PARAMETER);
            System.exit(2);
        }

        try (Crud crud = connect(config, output)) {
            if (config.showDeltaFor() != null) {
                try {
                    File file = new File(config.showDeltaFor());
                    if (!file.exists())
                        output.error(file.getName() + " does not exists.");
                    else if (file.isFile() && file.exists()) {
                        compareFile(file, config, output, crud);
                    } else if (file.isDirectory()) {
                        List<File> files = Arrays.asList(Objects.requireNonNull(file.listFiles(f -> f.getName().contains(config.showDeltaFor()) && f.getName().endsWith("." + FILE_EXTENSION))));
                        files.sort(Comparator.comparing(File::getName));
                        if (!files.isEmpty())
                            for (File f : files)
                                compareFile(f, config, output, crud);
                    }
                } catch (IOException e) {
                    output.error(e.getMessage());
                    System.exit(2);
                }

            } else if (config.getImportFile() != null) {
                File file = new File(config.getImportFile());
                if (!file.exists()) {
                    output.error(file.getName() + " does not exists.");
                    System.exit(2);
                } else if (file.isFile() && file.exists())
                    importFile(file, config, crud, output);
                else if (file.isDirectory()) {
                    List<File> files = Arrays.asList(Objects.requireNonNull(file.listFiles(f -> f.getName().contains(config.getImportFile()) && f.getName().endsWith("." + FILE_EXTENSION))));
                    files.sort(Comparator.comparing(File::getName));
                    if (!files.isEmpty()) {
                        output.userln("Files found: " + files);
                        if (output.question("   Importing " + files.size() + " files?", "Y", "n"))
                            for (File f : files)
                                importFile(f, config, crud, output);
                    }
                }
                if (!config.isAutocommit() && !config.isCommit())
                    try {
                        if (output.question("Committing changes?", "Y", "n"))
                            crud.commit();
                        else
                            crud.rollback();
                    } catch (SQLException e) {
                        output.error("Commit/rollback failed with error: " + e.getMessage() + " / " + e.getSQLState());
                    }

            } else if (config.getExportTable() != null) {
                try {
                    for (String table : crud.tables(config.getExportTable())) {
                        String filename = "." + File.separator + table.toLowerCase() + exportTimeAppendix(config) + "." + FILE_EXTENSION;
                        try {
                            output.userln("Export table " + table + " ");
                            Snapshot snapshot = crud.fetch(table, config.getExportWhere());
                            output.userln("   " + snapshot.getRecords().size() + " rows to file " + filename);
                            snapshot.export(Files.newOutputStream(Paths.get(filename)));
                        } catch (SQLException e) {
                            output.error("   Error: " + e.getMessage());
                        } catch (IOException e) {
                            output.error(e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    crud.rollback();
                    output.error(e.getMessage() + "\n" + e.getSQLState());
                    System.exit(2);
                }

            } else output.error("No usable parameters given!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Crud connect(Config config, OutPut output) {
        switch (config.getVendor()) {
            case "oracle":
                return Crud.connectOracle(config.getHostname(), config.getPort(), config.getServicename(), config.getUser(), config.getPassword(), config.isAutocommit());
            case "mysql":
                return Crud.connectMySql(config.getHostname(), config.getPort(), config.getServicename(), config.getUser(), config.getPassword(), config.isAutocommit());
            case "h2":
                return Crud.connectH2(config.isAutocommit());
            case "hsql":
                return Crud.connectHSQL(config.isAutocommit());
            default:
                output.error("Unknown vendor: \"" + config.getVendor() + "\"");
                output.error("   Try a known vendor: 'oracle', 'mysql', 'h2', 'hsql'");
                System.exit(3);
                return null;
        }
    }

    private static String exportTimeAppendix(Config config) {
        return config.isExportTime() ? "_" + EXPORT_DATE_FORMAT.format(new Date()) : "";
    }

    private static void compareFile(File file, Config config, OutPut output, Crud crud) throws IOException {
        output.user("Comparing reference file " + file);
        Snapshot reference = Snapshot.read(file);
        output.userln(" (" + reference.getRecords().size() + " records) to table " + reference.getTableName() + (reference.getWhere() != null ? " with condition " + reference.getWhere() : ""));

        if (crud.existsOrCreate(reference, false))
            try {
                ChangeSet change = crud.delta(reference, config.getIgnoreColumns());
                change.displayDiff(config.isVerbose());
            } catch (SQLException e) {
                output.error("   Error: " + e.getMessage());
                e.printStackTrace();
            }
        else if (reference.isEmpty())
            output.userln("   Reference is empty and table does not exist. Table could be dropped: 'drop table " + reference.getTableName() + "';");
        else
            output.error("   Error: Table " + reference.getTableName() + " does not exist!");
    }

    private static void importFile(File file, Config config, Crud crud, OutPut output) throws IOException {
        Snapshot reference = Snapshot.read(file);
        output.userln("Importing reference data from " + file + " into table " + reference.getTableName() + (reference.getWhere() != null ? " with condition " + reference.getWhere() : ""));

        if (crud.existsOrCreate(reference, !reference.isEmpty() && config.isForceInsert()))
            try {
                ChangeSet change = crud.delta(reference, config.getIgnoreColumns());
                if (change.isEmpty())
                    output.userln("   No differences found");
                else {
                    List<String> sqlUndoStmt = crud.apply(change, config.isCommit(), config.isContinueOnError());
                    if (config.isUndolog())
                        writeUndoLogs(change.table(), sqlUndoStmt);
                }
            } catch (SQLException e) {
                output.error("   Error: " + e.getMessage());
                e.printStackTrace();
            }
        else if (reference.isEmpty())
            output.error("   Reference is empty but table still exists!");
        else
            output.error("   Error: Table " + reference.getTableName() + " does not exist!");
    }

    private static void writeUndoLogs(String table, List<String> sqlUndoStmt) throws IOException {
        File undo = new File("." + File.separator + table.toLowerCase() + "_" + EXPORT_DATE_FORMAT.format(new Date()) + ".undo");
        Writer writer = new BufferedWriter(new FileWriter(undo));
        writer.write("-- Undo logs for table " + table + "\n");
        for (String stmt : sqlUndoStmt)
            writer.write(stmt + "\n");
        writer.close();
    }

}

package de.crud;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;

public class Starter {

    private static final String LOGO = "  _____  ____                 _ _\n" +
            " |  __ \\|  _ \\     /\\        | | |\n" +
            " | |  | | |_) |   /  \\    ___| | |_ __ _\n" +
            " | |  | |  _ <   / /\\ \\  / _ \\ | __/ _`|\n" +
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
            output.user(LOGO);
            output.user(Config.COMMAND_LINE_PARAMETER);
            System.exit(0);
        }

        output.user("DBΔelta");

        if (config.getVendor() == null) {
            output.error("\nNo vendor given!");
            output.error(Config.COMMAND_LINE_PARAMETER);
            System.exit(2);
        }

        Crud crud = null;
        try {
            switch (config.getVendor()) {
                case "oracle":
                    crud = Crud.connectOracle(config.getHostname(), config.getPort(), config.getServicename(), config.getUser(), config.getPassword(), config.isAutocommit());
                    break;
                case "mysql":
                    crud = Crud.connectMySql(config.getHostname(), config.getPort(), config.getServicename(), config.getUser(), config.getPassword(), config.isAutocommit());
                    break;
                case "h2":
                    crud = Crud.connectH2(config.isAutocommit());
                    break;
                case "hsql":
                    crud = Crud.connectHSQL(config.isAutocommit());
                    break;
                default:
                    output.error("Unknown vendor: \"" + config.getVendor() + "\"");
                    output.error("Try a known vendor: 'oracle', 'mysql', 'h2', 'hsql'");
                    System.exit(3);
            }

            if (config.showDeltaFor() != null) {
                try {
                    File file = new File(config.showDeltaFor());
                    if (!file.exists()) {
                        output.error(file.getName() + " does not exists.");
                        System.exit(2);
                    } else {
                        Snapshot reference = Snapshot.read(file);
                        Snapshot after = crud.fetch(reference.getTable(), reference.getWhere());
                        ChangeSet change = reference.delta(after);
                        change.displayDiff(config.isVerbose());
                    }
                } catch (SQLException e) {
                    output.error(e.getMessage() + "\n" + e.getSQLState());
                    System.exit(2);
                } catch (IOException e) {
                    output.error(e.getMessage());
                    System.exit(2);
                }

            } else if (config.getImportFile() != null) {
                File file = new File(config.getImportFile());
                if (file.isFile() && file.exists()) {
                    if (output.question("   Importing " + file.getName() + "?", "Y", "n"))
                        importFile(file, config, crud, output);
                } else {
                    File dir = new File(".");
                    List<File> files = Arrays.asList(Objects.requireNonNull(dir.listFiles(f -> f.getName().contains(config.getImportFile()) && f.getName().endsWith("." + FILE_EXTENSION))));
                    files.sort(Comparator.comparing(File::getName));
                    if (files.size() > 0 && output.question("   Importing " + files + "?", "Y", "n"))
                        for (File value : files)
                            importFile(value, config, crud, output);
                }
                if (!config.isAutocommit() && !config.isCommit()) {
                    try {
                        if (output.question("Committing changes?", "Y", "n"))
                            crud.commit();
                        else
                            crud.rollback();
                    } catch (SQLException e) {
                        output.error("Commit/rollback failed with error: " + e.getMessage() + " / " + e.getSQLState());
                    }
                }

            } else if (config.getExportTable() != null) {
                String filename = "." + File.separator + config.getExportTable().toLowerCase() + (config.isExportTime() ? "_" + EXPORT_DATE_FORMAT.format(new Date()) : "") + "." + FILE_EXTENSION;
                try {
                    Snapshot snapshot = crud.fetch(config.getExportTable(), config.getExportWhere());
                    snapshot.export(new FileOutputStream(filename));
                    output.user("Exported table \"" + config.getExportTable() + "\" to file " + filename);
                } catch (FileNotFoundException e) {
                    output.error("File " + filename + " not found.");
                } catch (SQLException e) {
                    output.error(e.getMessage() + "\n" + e.getSQLState());
                    System.exit(2);
                } catch (IOException e) {
                    output.error(e.getMessage());
                    System.exit(2);
                }

            } else if (config.getExportAllTables() != null) {
                try {
                    for (String table : crud.tables(config.getExportAllTables())) {
                        String filename = "." + File.separator + table.toLowerCase() + (config.isExportTime() ? "_" + EXPORT_DATE_FORMAT.format(new Date()) : "") + "." + FILE_EXTENSION;
                        try {
                            Snapshot snapshot = crud.fetch(table, config.getExportWhere());
                            snapshot.export(new FileOutputStream(filename));
                            output.user("Export table \"" + table + "\" to file " + filename);
                        } catch (FileNotFoundException e) {
                            output.error("File " + filename + " not found.");
                        } catch (IOException e) {
                            output.error(e.getMessage());
                        } catch (SQLException e) {
                            output.error("Error: " + table + ": " + e.getMessage() + "\n" + e.getSQLState());
                        }
                    }
                } catch (SQLException e) {
                    crud.rollback();
                    output.error(e.getMessage() + "\n" + e.getSQLState());
                    System.exit(2);
                }

            } else output.error("No usable parameters given!");
        } catch (Exception e) {
            try {
                if (crud != null) {
                    crud.rollback();
                }
            } catch (SQLException ex) {
                output.error("Rollback failed: " + ex.getMessage() + " / " + ex.getSQLState());
            }
        } finally {
            try {
                if (crud != null) {
                    crud.close();
                }
            } catch (SQLException e) {
                output.error("Closing connection failed: " + e.getMessage() + " / " + e.getSQLState());
            }
        }
    }

    private static void importFile(File file, Config config, Crud crud, OutPut output) {
        try {
            output.user("Importing file " + file.getName());
            Snapshot reference = Snapshot.read(file);
            if (reference.isEmpty()) {
                output.error("   No data to import.");
                return;
            } else if (config.isForceInsert() && !crud.existsOrCreate(reference)) {
                output.error("   Table " + reference.getTable() + " does not exist or could not be created.");
                return;
            }

            ChangeSet change = crud.delta(reference, config.getIgnoreColumns());
            if (!change.isEmpty()) {
                change.displayDiff(config.isVerbose());
                List<String> sqlUndoStmt = crud.apply(change, config.isCommit(), config.isContinueOnError());
                if (!change.isEmpty() && config.isUndolog())
                    writeUndoLogs(change.table(), sqlUndoStmt);
            } else
                output.user("   No differences found for table " + change.table());
        } catch (SQLException e) {
            output.error(e.getMessage() + "\n" + e.getSQLState());
        } catch (FileNotFoundException e) {
            output.error("File " + config.getImportFile() + " not found.");
        } catch (IOException e) {
            output.error(e.getMessage());
        }
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

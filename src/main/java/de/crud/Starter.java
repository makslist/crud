package de.crud;

import com.sanityinc.jargs.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;

public class Starter {

    private static class Config {

        private static final String COMMAND_LINE_PARAMETER = "Usage: dbd [{-v,--verbose}] [{--vendor} vendor name]\n" +
                "           [{--hostname} url] [{--port} port number] [{--servicename} service name]\n" +
                "           [{--user} user name] [{--password} password] [{--commit} commit]\n" +
                "           [{-i, --import} path to reference file\n" +
                "           [{-f, --forceInsert} create table if necessary to insert]\n" +
                "           [{-d, --delta} path to reference file to compare]]\n" +
                "           [{-e, --export} name of the table to export]]\n" +
                "           [{-a, --alltables} pattern or name of the tables to export]]\n" +
                "           [{--time} add a timestamp to the filename]]\n" +
                "           [{-w, --where} where statement]";

        private static final String LOGO = "  _____  ____                 _ _\n" +
                " |  __ \\|  _ \\     /\\        | | |\n" +
                " | |  | | |_) |   /  \\    ___| | |_ __ _\n" +
                " | |  | |  _ <   / /\\ \\  / _ \\ | __/ _`|\n" +
                " | |__| | |_) | / /__\\ \\|  __/ | || (_| |\n" +
                " |_____/|____/ /________\\\\___|_|\\__\\__,_|\n";

        public static Config loadConfig() {
            Config config = new Config();
            try (InputStream input = Files.newInputStream(Paths.get("./config.properties"))) {
                Properties prop = new Properties();
                prop.load(input);

                config.verbose = Boolean.parseBoolean(prop.getProperty("verbose", "false"));

                config.vendor = prop.getProperty("vendor", null);
                config.hostname = prop.getProperty("hostname", null);
                config.port = Integer.parseInt(prop.getProperty("port", "-1"));
                config.servicename = prop.getProperty("servicename", null);
                config.user = prop.getProperty("user", null);
                config.password = prop.getProperty("password", null);
                config.commit = Boolean.parseBoolean(prop.getProperty("commit", "false"));

                config.forceInsert = Boolean.parseBoolean(prop.getProperty("forceInsert", "false"));
                config.ignoreColumns.addAll(Arrays.asList(prop.getProperty("ignoreColumns", "").split(",")));
                config.exportTime = Boolean.parseBoolean(prop.getProperty("timestamp", "false"));
            } catch (IOException ex) {
                return config;
            }
            return config;
        }

        private static Config parseArgs(String[] args) {
            CmdLineParser parser = new CmdLineParser();
            CmdLineParser.Option<Boolean> verbose = parser.addBooleanOption('v', "verbose");
            CmdLineParser.Option<Boolean> help = parser.addBooleanOption('h', "help");

            CmdLineParser.Option<String> vendor = parser.addStringOption("vendor");

            CmdLineParser.Option<String> hostname = parser.addStringOption("hostname");
            CmdLineParser.Option<Integer> port = parser.addIntegerOption("port");
            CmdLineParser.Option<String> servicename = parser.addStringOption("servicename");
            CmdLineParser.Option<String> user = parser.addStringOption("user");
            CmdLineParser.Option<String> password = parser.addStringOption("password");
            CmdLineParser.Option<Boolean> commit = parser.addBooleanOption("commit");

            CmdLineParser.Option<String> importFile = parser.addStringOption('i', "import");
            CmdLineParser.Option<Boolean> forceInsert = parser.addBooleanOption('f', "forceInsert");
            CmdLineParser.Option<String> ignoreColumns = parser.addStringOption("ignoreColumns");

            CmdLineParser.Option<String> exportTable = parser.addStringOption('e', "export");
            CmdLineParser.Option<String> exportAllTables = parser.addStringOption('a', "alltables");
            CmdLineParser.Option<Boolean> exportTime = parser.addBooleanOption("timestamp");
            CmdLineParser.Option<String> exportWhere = parser.addStringOption('w', "where");

            CmdLineParser.Option<String> showDeltaFor = parser.addStringOption('d', "delta");

            try {
                parser.parse(args);
            } catch (CmdLineParser.OptionException e) {
                System.err.println(e.getMessage());
                System.err.println(COMMAND_LINE_PARAMETER);
                System.exit(2);
            }

            Config config = new Config();
            config.verbose = parser.getOptionValue(verbose, false);
            config.help = parser.getOptionValue(help, false);

            config.vendor = parser.getOptionValue(vendor, null);

            config.hostname = parser.getOptionValue(hostname, null);
            config.port = parser.getOptionValue(port, -1);
            config.servicename = parser.getOptionValue(servicename, null);
            config.user = parser.getOptionValue(user, null);
            config.password = parser.getOptionValue(password, null);
            config.commit = parser.getOptionValue(commit, false);

            config.importFile = parser.getOptionValue(importFile, null);
            config.forceInsert = parser.getOptionValue(forceInsert, false);
            config.ignoreColumns = new ArrayList<>(Arrays.asList(parser.getOptionValue(ignoreColumns, "").split(",")));

            config.exportTable = parser.getOptionValue(exportTable, null);
            config.exportAllTables = parser.getOptionValue(exportAllTables, null);
            config.exportTime = parser.getOptionValue(exportTime, false);
            config.exportWhere = parser.getOptionValue(exportWhere, null);

            config.showDeltaFor = parser.getOptionValue(showDeltaFor, null);

            return config;
        }

        private Config merge(Config config) {
            verbose = verbose || config.verbose;
            help = help || config.help;

            vendor = vendor != null ? vendor : config.vendor;
            hostname = hostname != null ? hostname : config.hostname;
            port = port != -1 ? port : config.port;
            servicename = servicename != null ? servicename : config.servicename;
            user = user != null ? user : config.user;
            password = password != null ? password : config.password;
            commit |= config.commit;

            importFile = importFile != null ? importFile : config.importFile;
            forceInsert |= config.forceInsert;
            config.ignoreColumns.forEach(c -> {
                if (ignoreColumns.contains(c)) ignoreColumns.add(c);
            });
            exportTable = exportTable != null ? exportTable : config.exportTable;
            exportAllTables = exportAllTables != null ? exportAllTables : config.exportAllTables;
            exportTime |= config.exportTime;
            exportWhere = exportWhere != null ? exportWhere : config.exportWhere;

            showDeltaFor = showDeltaFor != null ? showDeltaFor : config.showDeltaFor;

            return this;
        }

        boolean verbose;
        boolean help;

        String vendor;

        String hostname;
        int port;
        String servicename;
        String user;
        String password;
        boolean commit;

        String importFile;
        boolean forceInsert;
        List<String> ignoreColumns = new ArrayList<>();
        String exportTable;
        String exportAllTables;
        boolean exportTime;
        String exportWhere;

        String showDeltaFor;

    }

    private static final SimpleDateFormat EXPORT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmm");

    public static void main(String[] args) {
        Config config = Config.parseArgs(args).merge(Config.loadConfig());

        OutPut output;
        if (config.verbose) output = OutPut.create(OutPut.Level.INFO);
        else output = OutPut.create(OutPut.Level.USER);

        if (config.help) {
            output.user(Config.LOGO);
            output.user(Config.COMMAND_LINE_PARAMETER);
        }

        if (config.vendor == null) {
            output.error("No vendor given!");
            output.error(Config.COMMAND_LINE_PARAMETER);
            System.exit(2);
        }
        Crud crud;
        switch (config.vendor) {
            case "oracle":
                crud = Crud.connectOracle(config.hostname, config.port, config.servicename, config.user, config.password);
                break;
            case "mysql":
                crud = Crud.connectMySql(config.hostname, config.port, config.servicename, config.user, config.password);
                break;
            case "h2":
                crud = Crud.connectH2();
                break;
            case "hsql":
                crud = Crud.connectHSQL();
                break;
            default:
                output.error("Unknown vendor: \"" + config.vendor + "\"");
                System.exit(3);
                return;
        }

        if (config.showDeltaFor != null) {
            try {
                File file = new File(config.showDeltaFor);
                if (!file.exists()) {
                    output.error(file.getName() + " does not exists.");
                    System.exit(2);
                } else {
                    Snapshot reference = Snapshot.read(file);
                    Snapshot after = crud.fetch(reference.getTable(), reference.getWhere());
                    ChangeSet change = reference.delta(after);
                    change.displayDiff();
                }
            } catch (SQLException e) {
                output.error(e.getMessage() + "\n" + e.getSQLState());
                System.exit(2);
            } catch (IOException e) {
                output.error(e.getMessage());
                System.exit(2);
            }
        } else if (config.exportTable != null) {
            String filename = "." + File.separator + config.exportTable.toLowerCase() + (config.exportTime ? "_" + EXPORT_DATE_FORMAT.format(new Date()) : "") + ".snapshot";
            try {
                Snapshot snapshot = crud.fetch(config.exportTable, config.exportWhere);
                snapshot.export(new FileOutputStream(filename));
                output.user("Exported table \"" + config.exportTable + "\" to file " + filename);
            } catch (FileNotFoundException e) {
                output.error("File " + filename + " not found.");
            } catch (SQLException e) {
                output.error(e.getMessage() + "\n" + e.getSQLState());
                System.exit(2);
            } catch (IOException e) {
                output.error(e.getMessage());
                System.exit(2);
            }
        } else if (config.exportAllTables != null) {
            try {
                for (String table : crud.tables(config.exportAllTables)) {
                    String filename = "." + File.separator + table.toLowerCase() + (config.exportTime ? "_" + EXPORT_DATE_FORMAT.format(new Date()) : "") + ".snapshot";
                    try {
                        Snapshot snapshot = crud.fetch(table, config.exportWhere);
                        snapshot.export(new FileOutputStream(filename));
                        output.user("Exported table \"" + table + "\" to file " + filename);
                    } catch (FileNotFoundException e) {
                        output.error("File " + filename + " not found.");
                    } catch (IOException e) {
                        output.error(e.getMessage());
                    } catch (SQLException e) {
                        output.error("Error: " + table + ": " + e.getMessage() + "\n" + e.getSQLState());
                    }
                }
            } catch (SQLException e) {
                output.error(e.getMessage() + "\n" + e.getSQLState());
                System.exit(2);
            }
        } else if (config.importFile != null) {
            try {
                Snapshot reference = Snapshot.read(new File(config.importFile));
                if (!reference.isEmpty() && config.forceInsert && crud.existsOrCreate(reference)) {
                    output.error("No data to import in file " + config.importFile);
                    System.exit(2);
                }

                ChangeSet change = crud.delta(reference, config.ignoreColumns);
                if (!change.isEmpty()) {
                    change.displayDiff();
                    output.user("Apply diff to database? [Y/n]");
                    Scanner scanner = new Scanner(System.in);
                    String answer = scanner.nextLine();
                    if ("".equals(answer) || "Y".equalsIgnoreCase(answer)) {
                        crud.apply(change, config.commit);

                        if (!change.isEmpty())
                            output.user("-- Undo logs:");
                        for (String stmt : change.sqlUndoStmt())
                            output.user(stmt);
                    }
                } else output.user("No differences found.");
            } catch (SQLException e) {
                output.error(e.getMessage() + "\n" + e.getSQLState());
                System.exit(2);
            } catch (FileNotFoundException e) {
                output.error("File " + config.importFile + " not found.");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else output.error("No usable parameters given!");
    }

}

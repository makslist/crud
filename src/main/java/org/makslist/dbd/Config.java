package org.makslist.dbd;

import com.sanityinc.jargs.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Config {

    public static final String COMMAND_LINE_PARAMETER = "Usage: dbd [{-v,--verbose}] [{--vendor} vendor name]\n" +
            "           [h2, hsql, mysql, oracle, postgres]\n" +
            "           [{--hostname} url] [{--port} port number] [{--servicename} service name]\n" +
            "           [{--user} user name] [{--password} password] [{--commit} commit]\n" +
            "           [{-i, --import} file or path to reference file(s)]\n" +
            "               [{-u, --undolog} save undo log]\n" +
            "               [{-c, --continueOnError} continue on error]\n" +
            "               [{-f, --force} create table if it does not exist]\n" +
            "               [{--ignoreColumns} ignore columns when comparing]\n" +
            "           [{-d, --delta} file or path to reference file(s)]\n" +
            "               [{--ignoreColumns} ignore columns when comparing]\n" +
            "           [{-e, --export} name (incl. wildcards) of the table(s) entries to export]\n" +
            "               [{-w, --where} where statement]\n" +
            "               [{--timestamp} add a timestamp to the filename]\n" +
            "           [{--table} exports table metadata; name (incl. wildcards) of the table(s) to export]\n" +
            "           [{--view} exports view metadata; name (incl. wildcards) of the table(s) to export]\n" +
            "           [{--procedure} exports procedure metadata; name (incl. wildcards) of the table(s) to export]\n";

    private static Config loadConfig() {
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

            config.continueOnError = Boolean.parseBoolean(prop.getProperty("continueOnError", "false"));
            config.autocommit = Boolean.parseBoolean(prop.getProperty("autocommit", "false"));
            config.commit = Boolean.parseBoolean(prop.getProperty("commit", "false"));

            config.undolog = Boolean.parseBoolean(prop.getProperty("undolog", "false"));
            config.forceInsert = Boolean.parseBoolean(prop.getProperty("forceInsert", "false"));
            String ignoreColumnsOption = prop.getProperty("ignoreColumns", "");
            config.ignoreColumns = ignoreColumnsOption.isEmpty() ? new ArrayList<>() : new ArrayList<>(Arrays.asList(ignoreColumnsOption.split(",")));
            config.exportTime = Boolean.parseBoolean(prop.getProperty("timestamp", "false"));
        } catch (IOException ex) {
            return config;
        }
        return config;
    }

    static Config parseArgs(String[] args) {
        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option<Boolean> verbose = parser.addBooleanOption('v', "verbose");
        CmdLineParser.Option<Boolean> help = parser.addBooleanOption('h', "help");

        CmdLineParser.Option<String> vendor = parser.addStringOption("vendor");

        CmdLineParser.Option<String> hostname = parser.addStringOption("hostname");
        CmdLineParser.Option<Integer> port = parser.addIntegerOption("port");
        CmdLineParser.Option<String> servicename = parser.addStringOption("servicename");
        CmdLineParser.Option<String> user = parser.addStringOption("user");
        CmdLineParser.Option<String> password = parser.addStringOption("password");

        CmdLineParser.Option<Boolean> continueOnError = parser.addBooleanOption("continueOnError");
        CmdLineParser.Option<Boolean> autocommit = parser.addBooleanOption("autocommit");
        CmdLineParser.Option<Boolean> commit = parser.addBooleanOption('c', "commit");

        CmdLineParser.Option<String> importFile = parser.addStringOption('i', "import");
        CmdLineParser.Option<Boolean> undolog = parser.addBooleanOption('r', "undolog");
        CmdLineParser.Option<Boolean> forceInsert = parser.addBooleanOption('f', "forceInsert");
        CmdLineParser.Option<String> ignoreColumns = parser.addStringOption("ignoreColumns");

        CmdLineParser.Option<String> exportTable = parser.addStringOption('e', "export");
        CmdLineParser.Option<Boolean> exportTime = parser.addBooleanOption("timestamp");
        CmdLineParser.Option<String> exportWhere = parser.addStringOption('w', "where");

        CmdLineParser.Option<String> showDeltaFor = parser.addStringOption('d', "delta");

        CmdLineParser.Option<String> table = parser.addStringOption("table");
        CmdLineParser.Option<String> view = parser.addStringOption("view");
        CmdLineParser.Option<String> procedure = parser.addStringOption("procedure");

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

        config.continueOnError = parser.getOptionValue(continueOnError, false);
        config.autocommit = parser.getOptionValue(autocommit, false);
        config.commit = parser.getOptionValue(commit, false);

        config.importFile = parser.getOptionValue(importFile, null);
        config.undolog = parser.getOptionValue(undolog, false);
        config.forceInsert = parser.getOptionValue(forceInsert, false);
        String ignoreColumnsOption = parser.getOptionValue(ignoreColumns, "");
        config.ignoreColumns = ignoreColumnsOption.isEmpty() ? new ArrayList<>() : new ArrayList<>(Arrays.asList(ignoreColumnsOption.split(",")));

        config.exportTable = parser.getOptionValue(exportTable, null);
        config.exportTime = parser.getOptionValue(exportTime, false);
        config.exportWhere = parser.getOptionValue(exportWhere, null);

        config.showDeltaFor = parser.getOptionValue(showDeltaFor, null);

        config.table = parser.getOptionValue(table, null);
        config.view = parser.getOptionValue(view, null);
        config.procedure = parser.getOptionValue(procedure, null);

        return config.merge(Config.loadConfig()); // args have precedence over config
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

        continueOnError |= config.continueOnError;
        autocommit |= config.autocommit;
        commit |= config.commit;

        importFile = importFile != null ? importFile : config.importFile;
        undolog |= config.undolog;
        forceInsert |= config.forceInsert;
        config.ignoreColumns.forEach(c -> {
            if (!ignoreColumns.contains(c)) ignoreColumns.add(c);
        });
        exportTable = exportTable != null ? exportTable : config.exportTable;
        exportTime |= config.exportTime;
        exportWhere = exportWhere != null ? exportWhere : config.exportWhere;

        showDeltaFor = showDeltaFor != null ? showDeltaFor : config.showDeltaFor;

        table = table != null ? table : config.table;
        view = view != null ? view : config.view;

        return this;
    }

    private boolean verbose;
    private boolean help;

    private String vendor;

    private String hostname;
    private int port;
    private String servicename;
    private String user;
    private String password;

    private boolean continueOnError;
    private boolean autocommit;
    private boolean commit;

    private String importFile;
    private boolean undolog;
    private boolean forceInsert;
    private List<String> ignoreColumns = new ArrayList<>();
    private String exportTable;
    private boolean exportTime;
    private String exportWhere;
    private String showDeltaFor;
    private String table;
    private String view;
    private String procedure;

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isHelp() {
        return help;
    }

    public String getVendor() {
        return vendor;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getServicename() {
        return servicename;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isContinueOnError() {
        return continueOnError;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public boolean isCommit() {
        return commit;
    }

    public String getImportFile() {
        return importFile;
    }

    public boolean isUndolog() {
        return undolog;
    }

    public boolean isForceInsert() {
        return forceInsert;
    }

    public List<String> getIgnoreColumns() {
        return ignoreColumns;
    }

    public String getExportTable() {
        return exportTable;
    }

    public boolean isExportTime() {
        return exportTime;
    }

    public String getExportWhere() {
        return exportWhere;
    }

    public String showDeltaFor() {
        return showDeltaFor;
    }

    public String tableMeta() {
        return table;
    }

    public String viewMeta() {
        return view;
    }

    public String procedureMeta() {
        return procedure;
    }

}
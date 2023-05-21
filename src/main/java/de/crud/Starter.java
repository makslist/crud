package de.crud;

import com.sanityinc.jargs.*;

import java.io.*;
import java.nio.file.*;
import java.text.*;
import java.util.*;

public class Starter {

    private static class Config {

        public static Config loadConfig() {
            Config config = new Config();
            try (InputStream input = Files.newInputStream(Paths.get("src/main/resources/config.properties"))) {
                Properties prop = new Properties();
                prop.load(input);

                config.verbose = Boolean.parseBoolean(prop.getProperty("out.verbose", "false"));

                config.vendor = prop.getProperty("db.vendor", null);
                config.hostname = prop.getProperty("db.hostname", null);
                config.port = Integer.parseInt(prop.getProperty("db.port", "-1"));
                config.servicename = prop.getProperty("db.servicename", null);
                config.user = prop.getProperty("db.user", null);
                config.password = prop.getProperty("db.password", null);
                config.commit = Boolean.parseBoolean(prop.getProperty("db.commit", "false"));

                config.forceInsert = Boolean.parseBoolean(prop.getProperty("import.forceInsert", "false"));
                config.ignoreColumns.addAll(Arrays.asList(prop.getProperty("import.ignoreColumns", "").split(",")));
                config.exportTime = Boolean.parseBoolean(prop.getProperty("export.timestamp", "false"));
            } catch (IOException ex) {
                return config;
            }
            return config;
        }

        private static Config parseArgs(String[] args) {
            CmdLineParser parser = new CmdLineParser();
            CmdLineParser.Option<Boolean> verbose = parser.addBooleanOption('v', "verbose");

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

            CmdLineParser.Option<String> exportTable = parser.addStringOption('t', "table");
            CmdLineParser.Option<Boolean> exportTime = parser.addBooleanOption("timestamp");
            CmdLineParser.Option<String> exportWhere = parser.addStringOption('w', "where");

            try {
                parser.parse(args);
            } catch (CmdLineParser.OptionException e) {
                System.err.println(e.getMessage());
                System.err.println("Usage: crud [{-v,--verbose}] [{--vendor} vendor name]\n"
                        + "[{--hostname} url] [{--port} port number] [{--servicename} service name]\n"
                        + "[{--user} user name] [{--password} password] [{--commit} commit]\n"
                        + "[{-i, --import} path to reference file\n"
                        + "[{-f, --forceInsert} create table if necessary to insert]\n"
                        + "[{-t, --table} name of the table to export or compare]]\n"
                        + "[{--time} add a timestamp to the filename]]\n"
                        + "[{-w, --where} where statement]");
                System.exit(2);
            }

            Config config = new Config();

            config.verbose = parser.getOptionValue(verbose, false);

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
            config.exportTime = parser.getOptionValue(exportTime, false);
            config.exportWhere = parser.getOptionValue(exportWhere, null);

            return config;
        }

        private Config merge(Config config) {
            verbose = verbose || config.verbose;

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
                if (ignoreColumns.contains(c))
                    ignoreColumns.add(c);
            });
            exportTable = exportTable != null ? exportTable : config.exportTable;
            exportTime |= config.exportTime;
            exportWhere = exportWhere != null ? exportWhere : config.exportWhere;
            return this;
        }

        boolean verbose;

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
        boolean exportTime;
        String exportWhere;

    }

    private static final SimpleDateFormat exportDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");

    public static void main(String[] args) {
        Config config = Config.parseArgs(args).merge(Config.loadConfig());

        OutPut output;
        if (config.verbose) output = OutPut.create(OutPut.Level.INFO);
        else output = OutPut.create(OutPut.Level.USER);

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
                throw new IllegalStateException("Unexpected value: " + config.vendor);
        }

        if (config.exportTable != null) {
            Snapshot snapshot = crud.fetch(config.exportTable, config.exportWhere);
            try {
                String filename = "./" + config.exportTable + (config.exportTime ? "_" + exportDateFormat.format(new Date()) : "") + ".snapshot";
                snapshot.export(new FileOutputStream(filename));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("File not found!");
            }
        } else if (config.importFile != null) {
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

                    output.user("-- Undo logs");
                    for (String stmt : change.sqlUndoStmt())
                        output.user(stmt);
                }
            } else output.user("No differences found.");
        } else output.error("No usable parameters given!");
    }

}

package de.crud;

import com.sanityinc.jargs.CmdLineParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Starter {

    private static final SimpleDateFormat exportDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");

    public static void main(String[] args) {
        CmdLineParser parser = new CmdLineParser();
        CmdLineParser.Option<Boolean> debug = parser.addBooleanOption('d', "debug");
        CmdLineParser.Option<Boolean> verbose = parser.addBooleanOption('v', "verbose");
        CmdLineParser.Option<String> db = parser.addStringOption("db");
        CmdLineParser.Option<String> hostname = parser.addStringOption("hostname");
        CmdLineParser.Option<Integer> port = parser.addIntegerOption('p', "port");
        CmdLineParser.Option<String> servicename = parser.addStringOption('s', "servicename");
        CmdLineParser.Option<String> user = parser.addStringOption("user");
        CmdLineParser.Option<String> password = parser.addStringOption("password");
        CmdLineParser.Option<String> paramRefFile = parser.addStringOption('f', "file");
        CmdLineParser.Option<String> paramExportTable = parser.addStringOption('t', "table");

        OutPut output;
        try {
            parser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            output = OutPut.create(OutPut.Level.USER);
            output.error(e.getMessage());
            output.error("""
                    Usage: crud [-d,--debug] [{-v,--verbose}] [{-h, --help}] [{-d,--db} system]
                                      [{--hostname} url] [{-p,--port} port number] [{-s, --servicename} service name]
                                      [{--user} user name] [{--password} password]
                                      [{-f, --file} path to reference file [{-t,--table} name of the table to export or compare]]""");
            System.exit(2);
        }

        if (parser.getOptionValue(debug, false)) output = OutPut.create(OutPut.Level.DEBUG);
        else if (parser.getOptionValue(verbose, false)) output = OutPut.create(OutPut.Level.INFO);
        else output = OutPut.create(OutPut.Level.USER);

        Crud crud = switch (parser.getOptionValue(db)) {
            case "mysql" ->
                    Crud.connectMySql(parser.getOptionValue(hostname), parser.getOptionValue(port), parser.getOptionValue(servicename), parser.getOptionValue(user), parser.getOptionValue(password));
            case "oracle" ->
                    Crud.connectOracle(parser.getOptionValue(hostname), parser.getOptionValue(port), parser.getOptionValue(servicename), parser.getOptionValue(user), parser.getOptionValue(password));
            case "h2" -> Crud.connectH2();
            case "hsql" -> Crud.connectHSQL();
            default -> throw new IllegalStateException("Unexpected value: " + parser.getOptionValue(db));
        };

        String exportTable = parser.getOptionValue(paramExportTable, null);
        String file = parser.getOptionValue(paramRefFile, null);
        if (exportTable != null) {
            Snapshot snap = crud.fetchTable(exportTable);
            try {
                snap.export(new FileOutputStream("./" + exportTable + "_" + exportDateFormat.format(new Date()) + ".snapshot"));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("File not found!");
            }
        } else if (file != null) {
            Snapshot reference = crud.read(new File(file));
            Snapshot current = crud.fetch(reference.getTable(), reference.getWhere());
            ChangeSet change = reference.diff(current, new ArrayList<>());

            if (change.isEmpty()) {
                change.displayDiff();
                output.user("Apply diff to database? [Y/n]");
                Scanner scanner = new Scanner(System.in);
                String answer = scanner.nextLine();
                if ("".equals(answer) || "Y".equalsIgnoreCase(answer)) crud.apply(change);
            } else output.user("No differences found.");
        } else output.error("No usable parameters given!");
    }

}

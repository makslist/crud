package de.crud;

import oracle.jdbc.pool.*;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;
import java.util.stream.*;

import static java.sql.Types.*;

public class Crud {

    public static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DecimalFormat DECIMAL_FORMAT;

    private static OutPut output = OutPut.getInstance();

    static {
        DECIMAL_FORMAT = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        DECIMAL_FORMAT.setMaximumFractionDigits(340);
    }

    public static Crud connectHSQL() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            return new Crud("sa", DriverManager.getConnection("jdbc:hsqldb:mem:myDb", "sa", ""));
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Crud connectMySql(String hostname, int port, String serviceName, String user, String password) {
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
            return new Crud(user, DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + serviceName, user, password));
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Crud connectH2() {
        try {
            return new Crud("sa", DriverManager.getConnection("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;NON_KEYWORDS=KEY,VALUE", "sa", "sa"));
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public static Crud connectOracle(String hostname, int port, String serviceName, String user, String password) {
        try {
            OracleDataSource ods = new OracleDataSource();
            ods.setURL("jdbc:oracle:thin:@//" + hostname + ":" + port + "/" + serviceName);
            ods.setUser(user);
            ods.setPassword(password);
            return new Crud(user, ods.getConnection());
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    private final Connection conn;
    private final boolean isMixedCase;

    private final String user;

    private Crud(String user, Connection conn) throws SQLException {
        this.user = user;
        this.conn = conn;
        this.conn.setAutoCommit(false);
        this.isMixedCase = conn.getMetaData().storesMixedCaseIdentifiers();
        Crud.output = OutPut.create(OutPut.Level.USER);
        output.user("Connection established...");
    }

    public void execute(String sql) throws SQLException {
        output.debug(sql);
        conn.prepareStatement(sql).executeUpdate();
    }

    public void close() throws SQLException {
        conn.close();
    }

    public List<String> tables(String pattern) throws SQLException {
        List<String> result = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        String[] types = {"TABLE"};
        ResultSet tables = metaData.getTables(null, user.toUpperCase(), pattern == null || pattern.isEmpty() ? "%" : pattern.toUpperCase(), types);
        while (tables.next())
            result.add(tables.getString("TABLE_NAME"));
        return result;
    }

    public List<String> descPkOf(String table) throws SQLException {
        boolean isMixedCase = conn.getMetaData().storesMixedCaseIdentifiers();
        ResultSet rsPkUser = conn.getMetaData().getPrimaryKeys(null, user.toUpperCase(), isMixedCase ? table : table.toUpperCase());
        List<String> pkColumns = new ArrayList<>();
        while (rsPkUser.next()) {
            String columnName = rsPkUser.getString("COLUMN_NAME");
            pkColumns.add(isMixedCase ? columnName : columnName.toLowerCase());
        }
        if (!pkColumns.isEmpty())
            return pkColumns;

        ResultSet rsPk = conn.getMetaData().getPrimaryKeys(null, null, isMixedCase ? table : table.toUpperCase());
        String schema = null;
        while (rsPk.next() && (schema == null || schema.equals(rsPk.getString("TABLE_SCHEM")))) {
            schema = rsPk.getString("TABLE_SCHEM");
            String columnName = rsPk.getString("COLUMN_NAME");
            pkColumns.add(isMixedCase ? columnName : columnName.toLowerCase());
        }
        return pkColumns;
    }

    public Snapshot fetch(String table) throws SQLException {
        return fetch(table, null);
    }

    public ChangeSet delta(Snapshot snapshot, List<String> ignoreColumns) throws SQLException {
        Snapshot current = fetch(snapshot.getTable(), snapshot.getWhere());
        return snapshot.delta(current, ignoreColumns);
    }

    public boolean existsOrCreate(Snapshot snapshot) throws SQLException {
        try {
            PreparedStatement stmt = conn.prepareStatement("select * from " + snapshot.getTable() + " where 1 = 2");
            return stmt.execute();
        } catch (SQLException e) {
            String sql = "create table " + snapshot.getTable() + " (" + snapshot.columns().map(c -> c + " " + (snapshot.getColumnTypes().get(c).getSql())).collect(Collectors.joining(", ")) + ", primary key (" + snapshot.pkColumns().collect(Collectors.joining(", ")) + "))";
            output.user("   Table " + snapshot.getTable() + " does not exist. Trying to create.");
            output.info(sql);
            return conn.prepareStatement(sql).execute();
        }
    }

    public Snapshot fetch(String table, String whereStmt) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("select * from " + table + (whereStmt != null ? " where " + whereStmt : ""));
        stmt.setFetchSize(1000);
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        Map<String, Snapshot.SqlType> columnTypes = new HashMap<>();
        String[] columns = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String columnName = isMixedCase ? rsmd.getColumnName(i) : rsmd.getColumnName(i).toLowerCase();
            columnTypes.put(columnName, new Snapshot.SqlType(rsmd.getColumnType(i), rsmd.getPrecision(i), rsmd.getScale(i)));
            columns[i - 1] = columnName;
        }

        Snapshot snapshot = new Snapshot(table, columns, columnTypes, descPkOf(table), whereStmt);

        while (rs.next()) {
            String[] record = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                switch (rsmd.getColumnType(i)) {
                    case SMALLINT:
                        short sho = rs.getShort(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(sho);
                        break;
                    case TINYINT:
                    case INTEGER:
                        int in = rs.getInt(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(in);
                        break;
                    case BIGINT:
                        long lon = rs.getLong(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(lon);
                        break;
                    case NUMERIC:
                    case DECIMAL:
                        BigDecimal bigDecimal = rs.getBigDecimal(i);
                        record[i - 1] = rs.wasNull() ? null : DECIMAL_FORMAT.format(bigDecimal);
                        break;
                    case FLOAT:
                        float floa = rs.getFloat(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(floa);
                        break;
                    case REAL:
                    case DOUBLE:
                        double doub = rs.getDouble(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(doub);
                        break;
                    case BOOLEAN:
                        boolean bool = rs.getBoolean(i);
                        record[i - 1] = rs.wasNull() ? null : String.valueOf(bool);
                        break;
                    case NULL:
                        record[i - 1] = null;
                        break;
                    case DATE:
                        java.sql.Date date = rs.getDate(i);
                        record[i - 1] = rs.wasNull() ? null : SQL_DATE_FORMAT.format(date);
                        break;
                    case TIME:
                        Time time = rs.getTime(i);
                        record[i - 1] = rs.wasNull() ? null : time.toLocalTime().toString();
                        break;
                    case TIMESTAMP:
                        Timestamp timestamp = rs.getTimestamp(i);
                        record[i - 1] = rs.wasNull() ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format(new Date(timestamp.getTime()));
                        break;
                    case NCLOB:
                    case CLOB:
                        Clob clob = rs.getClob(i);
                        record[i - 1] = rs.wasNull() ? null : clob.getSubString(1, (int) clob.length());
                        break;
                    case BLOB:
                        Blob blob = rs.getBlob(i);
                        record[i - 1] = rs.wasNull() ? null : new String(Base64.getEncoder().encode(blob.getBytes(1L, (int) blob.length())));
                        break;
                    case SQLXML:
                        break;
                    case TIME_WITH_TIMEZONE:
                    case TIMESTAMP_WITH_TIMEZONE:
                    case BINARY:
                    case BIT:
                    default:
                        String string = rs.getString(i);
                        record[i - 1] = rs.wasNull() ? null : string;
                        break;
                }
            }
            snapshot.addRecord(record);
        }
        return snapshot;
    }

    public List<String> apply(ChangeSet changes, boolean commit) throws SQLException {
        if (!changes.insertRecs().isEmpty())
            output.user("   Inserting " + changes.insertRecs().size() + " rows");
        changes.applyInsert(conn);
        if (!changes.updateRecs().isEmpty())
            output.user("   Updating " + changes.updateRecs().size() + " rows");
        changes.applyUpdate(conn);
        if (!changes.deleteRecs().isEmpty())
            output.user("   Deleting " + changes.deleteRecs().size() + " rows");
        changes.applyDelete(conn);
        if (commit) execute("commit;");

        return changes.sqlUndoStmt();
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void write(ChangeSet changes, OutputStream out) {
        OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(out));
        try {
            for (String stmt : changes.sqlApplyStmt())
                writer.write(stmt + System.lineSeparator());
            writer.close();
        } catch (IOException e) {
            output.error("Failed writing: " + e.getMessage());
        }
    }

}

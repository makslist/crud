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

public class Crud implements AutoCloseable {

    public static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DecimalFormat DECIMAL_FORMAT;

    private static OutPut output = OutPut.getInstance();

    static {
        DECIMAL_FORMAT = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        DECIMAL_FORMAT.setMaximumFractionDigits(340);
    }

    public static Crud connectHSQL(boolean autocommit) {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            return new Crud("sa", DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "sa", ""), autocommit);
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Crud connectH2(boolean autocommit) {
        try {
            return new Crud("sa", DriverManager.getConnection("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1;NON_KEYWORDS=KEY,VALUE", "sa", "sa"), autocommit);
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    public static Crud connectMySql(String hostname, int port, String serviceName, String user, String password, boolean autocommit) {
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
            return new Crud(user, DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + serviceName, user, password), autocommit);
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static Crud connectOracle(String hostname, int port, String serviceName, String user, String password, boolean autocommit) {
        try {
            OracleDataSource ods = new OracleDataSource();
            ods.setURL("jdbc:oracle:thin:@//" + hostname + ":" + port + "/" + serviceName);
            ods.setUser(user);
            ods.setPassword(password);
            return new Crud(user, ods.getConnection(), autocommit);
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    private final Connection conn;
    private final boolean isMixedCase;

    private final String user;

    private Crud(String user, Connection conn, boolean autocommit) throws SQLException {
        this.user = user;
        this.conn = conn;
        this.conn.setAutoCommit(autocommit);
        this.isMixedCase = conn.getMetaData().storesMixedCaseIdentifiers();
        Crud.output = OutPut.getInstance();
        output.userln("Connection established to " + conn.getMetaData().getDatabaseProductName() + " " +
                conn.getMetaData().getDatabaseMajorVersion() + "." +
                conn.getMetaData().getDatabaseMinorVersion() + " (user: " + this.user + ")");
    }

    public void execute(String sql) throws SQLException {
        output.debug(sql);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
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

    public ChangeSet delta(Snapshot snapshot, List<String> ignoreColumns) throws SQLException {
        Snapshot current = fetch(snapshot.getTable(), snapshot.getWhere());
        return snapshot.delta(current, ignoreColumns);
    }

    public boolean existsOrCreate(Snapshot snapshot, boolean createTable) {
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet resultSet = meta.getTables(null, null, snapshot.getTable().toUpperCase(), new String[]{"TABLE"})) {
                if (resultSet.next())
                    return true;
                else if (createTable) {
                    output.info("   Table " + snapshot.getTable() + " does not exist. Trying to create.");
                    String sqlCreate = "create table " + snapshot.getTable() + " (" + snapshot.columns().map(c -> c + " " + (snapshot.getColumnTypes().get(c).getSql())).collect(Collectors.joining(", ")) + ", primary key (" + snapshot.pkColumns().collect(Collectors.joining(", ")) + "))";
                    try (PreparedStatement create = conn.prepareStatement(sqlCreate)) {
                        create.execute();
                        return true;
                    } catch (SQLException ex) {
                        output.error("      Creating table failed with: " + ex.getMessage());
                        output.error("      Statement: " + sqlCreate);
                    }
                }
            }
        } catch (SQLException e) {
            output.error(e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public Snapshot fetch(String table) throws SQLException {
        return fetch(table, null);
    }

    public Snapshot fetch(String table, String whereStmt) throws SQLException {
        String sql = "select * from " + table + (whereStmt != null ? " where " + whereStmt : "");
        try (PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            Map<String, Snapshot.SqlType> columnTypes = new HashMap<>();
            String[] columns = new String[columnCount];
            int[] types = new int[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                String columnName = isMixedCase ? rsmd.getColumnName(i) : rsmd.getColumnName(i).toLowerCase();
                columns[i - 1] = columnName;
                types[i - 1] = rsmd.getColumnType(i);
                columnTypes.put(columnName, new Snapshot.SqlType(rsmd.getColumnType(i), rsmd.getPrecision(i), rsmd.getScale(i)));
            }

            Snapshot snapshot = new Snapshot(table, columns, columnTypes, descPkOf(table), whereStmt);

            long rowCount = 0;
            while (rs.next()) {
                if (++rowCount % 100000 == 0)
                    output.userln("   " + rowCount + " rows so far");
                String[] record = new String[columnCount];

                for (int i = 1; i <= columnCount; i++)
                    switch (types[i - 1]) {
                        case BINARY:
                        case VARBINARY:
                        case LONGVARBINARY:
                            try {
                                byte[] bytes = rs.getBytes(i);
                                record[i - 1] = rs.wasNull() ? null : new String(Base64.getEncoder().encode(bytes));
                            } catch (SQLException e) {
                                record[i - 1] = null;
                            }
                            break;
                        default:
                            break;
                    }

                for (int i = 1; i <= columnCount; i++)
                    switch (types[i - 1]) {
                        case TINYINT:
                        case SMALLINT:
                            short sho = rs.getShort(i);
                            record[i - 1] = rs.wasNull() ? null : String.valueOf(sho);
                            break;
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
                        case BINARY:
                        case VARBINARY:
                        case LONGVARBINARY:
                            // get bytes as last
                            break;
                        case SQLXML:
                            output.error("Datatype: " + rsmd.getColumnType(i) + " is not supported.");
                            break;
                        case TIME_WITH_TIMEZONE:
                        case TIMESTAMP_WITH_TIMEZONE:
                        case BIT:
                        case BOOLEAN:
                            boolean bool = rs.getBoolean(i);
                            record[i - 1] = rs.wasNull() ? null : String.valueOf(bool);
                            break;
                        case OTHER:
                        case JAVA_OBJECT:
                        case ARRAY:
                        case STRUCT:
                        case DISTINCT:
                        case REF:
                            throw new RuntimeException("ResultSetSerializer not yet implemented for SQL type REF");
                        case NVARCHAR:
                        case VARCHAR:
                        case LONGNVARCHAR:
                        case LONGVARCHAR:
                        default:
                            String string = rs.getString(i);
                            record[i - 1] = rs.wasNull() ? null : string;
                            break;
                    }

                snapshot.addRecord(record);
            }
            return snapshot;
        }
    }

    public List<String> apply(ChangeSet changes, boolean commit, boolean continueOnError) throws SQLException {
        if (!changes.insertRecs().isEmpty())
            output.userln("   Inserting " + changes.insertRecs().size() + " rows");
        changes.applyInsert(conn, continueOnError);
        if (!changes.updateRecs().isEmpty())
            output.userln("   Updating " + changes.updateRecs().size() + " rows");
        changes.applyUpdate(conn, continueOnError);
        if (!changes.deleteRecs().isEmpty())
            output.userln("   Deleting " + changes.deleteRecs().size() + " rows");
        changes.applyDelete(conn, continueOnError);
        if (commit)
            commit();

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

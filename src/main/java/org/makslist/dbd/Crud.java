package org.makslist.dbd;

import oracle.jdbc.pool.*;
import org.postgresql.ds.*;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;

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

    public static Crud connectPostgres(String hostname, int port, String databaseName, String user, String password, boolean autocommit) {
        try {
            final PGSimpleDataSource dataSource = new PGSimpleDataSource();
            dataSource.setUrl("jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName);
            return new Crud(user, dataSource.getConnection(user, password), autocommit);
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
        String userName = metaData.getUserName();

        String[] types = {"TABLE"};
        ResultSet tables = metaData.getTables(null, user.toUpperCase(), pattern == null || pattern.isEmpty() ? "%" : pattern.toUpperCase(), types);
        while (tables.next())
            result.add(tables.getString("TABLE_NAME"));
        return result;
    }

    public TableMeta tableMetaData(String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String userName = metaData.getUserName();
        String tableRemarks = null;
        try (ResultSet resultSet = metaData.getTables(null, null, tableName.toUpperCase(), new String[]{"TABLE"})) {
            while (resultSet.next())
                tableRemarks = resultSet.getString("REMARKS");
        }

        List<TableMeta.Column> columns = new ArrayList<>();
        try (ResultSet column = metaData.getColumns(null, null, tableName.toUpperCase(), null)) {
            while (column.next()) {
                int position = column.getInt("ORDINAL_POSITION");
                String columnName = column.getString("COLUMN_NAME").toLowerCase();
                String remarks = column.getString("REMARKS");
                int datatype = column.getInt("DATA_TYPE");
                int columnSize = column.getInt("COLUMN_SIZE");
                int decimalDigits = column.getInt("DECIMAL_DIGITS");
                boolean isNullable = "YES".equals(column.getString("IS_NULLABLE"));
                boolean isAutoIncrement = "YES".equals(column.getString("IS_AUTOINCREMENT"));
                String defaultValue = null;//column.getString("COLUMN_DEF");
                columns.add(new TableMeta.Column(position, columnName.toLowerCase(), remarks, datatype, columnSize, decimalDigits, isNullable, isAutoIncrement, defaultValue));
            }
        }

        TableMeta.PrimaryKey pk = null;
        try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName.toUpperCase())) {
            String primaryKeyName = null;
            List<String> pkColumns = new ArrayList<>();
            while (primaryKeys.next()) {
                primaryKeyName = primaryKeys.getString("PK_NAME");
                pkColumns.add(primaryKeys.getString("COLUMN_NAME").toLowerCase());
            }
            if (primaryKeyName != null)
                pk = new TableMeta.PrimaryKey(primaryKeyName.toLowerCase(), pkColumns);
            else
                output.error("Table " + tableName + " has no primary key.");
        }

        List<TableMeta.ForeignKey> fks = new ArrayList<>();
        try (ResultSet foreignKeys = metaData.getImportedKeys(null, null, tableName.toUpperCase())) {
            String fkName = null;
            List<TableMeta.ForeignKey.ColumnMapping> mapping = new ArrayList<>();
            while (foreignKeys.next()) {
                fkName = foreignKeys.getString("FK_NAME");
                String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                mapping.add(new TableMeta.ForeignKey.ColumnMapping(pkTableName.toLowerCase(), pkColumnName.toLowerCase(), fkTableName.toLowerCase(), fkColumnName.toLowerCase()));
            }
            if (fkName != null)
                fks.add(new TableMeta.ForeignKey(fkName.toLowerCase(), mapping));
        }

        return new TableMeta(tableName.toLowerCase(), tableRemarks, columns, pk, fks);
    }

    public List<String> allViews(String pattern) throws SQLException {
        return View.all(user, pattern, conn);
    }

    public View view(String name) throws SQLException {
        return View.ddl(name, conn);
    }

    public List<String> allProcedures(String pattern) throws SQLException {
        return StoredProcedure.all(user, pattern, conn);
    }

    public StoredProcedure procedure(String name) throws SQLException {
        return StoredProcedure.ddl(user, name, conn);
    }

    public ChangeSet delta(Snapshot snapshot, List<String> ignoreColumns) throws SQLException {
        Snapshot current = fetch(snapshot.getTableName(), snapshot.getWhere());
        return snapshot.delta(current, ignoreColumns);
    }

    public boolean existsOrCreate(Snapshot snapshot, boolean createTable) {
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet resultSet = meta.getTables(null, null, snapshot.getTableName().toUpperCase(), new String[]{"TABLE"})) {
                if (resultSet.next())
                    return true;
                else if (createTable) {
                    output.info("   Table " + snapshot.getTableName() + " does not exist. Trying to create.");
                    try (PreparedStatement create = conn.prepareStatement(snapshot.getTable().createSql())) {
                        create.execute();
                        return true;
                    } catch (SQLException ex) {
                        output.error("      Creating table failed with: " + ex.getMessage());
                        output.error("      Statement: " + snapshot.getTable().createSql());
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
        TableMeta tableMeta = tableMetaData(table);

        String sql = "select * from " + table + (whereStmt != null ? " where " + whereStmt : "");
        try (PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = tableMeta.columns.size();

//            String columnName = isMixedCase ? rsmd.getColumnName(i) : rsmd.getColumnName(i).toLowerCase();

            Snapshot snapshot = new Snapshot(tableMeta, whereStmt);

            long rowCount = 0;
            while (rs.next()) {
                if (++rowCount % 100000 == 0)
                    output.userln("   " + rowCount + " rows so far");
                String[] record = new String[columnCount];

                for (int i = 1; i <= tableMeta.columns.size(); i++)
                    switch (tableMeta.columns.get(i - 1).datatype) {
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

                for (int i = 1; i <= tableMeta.columns.size(); i++)
                    switch (tableMeta.columns.get(i - 1).datatype) {
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
                            output.error("ResultSetSerializer not yet implemented for SQL type REF");
                            break;
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

package de.crud;

import oracle.jdbc.pool.*;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.Date;
import java.util.*;
import java.util.stream.*;

import static java.sql.Types.*;

public class Crud {

    public static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static OutPut output = OutPut.getInstance();

    public static Crud connectHSQL() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            return new Crud("sa", DriverManager.getConnection("jdbc:hsqldb:data/tutorial", "sa", ""));
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
            return new Crud("sa", DriverManager.getConnection("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1", "sa", "sa"));
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
    }

    public void execute(String sql) {
        try {
            output.debug(sql);
            conn.prepareStatement(sql).executeUpdate();
        } catch (SQLException e) {
            output.error("Failure when executing: " + sql + System.lineSeparator() + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> descPkOf(String table) {
        try {
            ResultSet rsPk = conn.getMetaData().getPrimaryKeys(null, null, table.toUpperCase());
            boolean isMixedCase = conn.getMetaData().storesMixedCaseIdentifiers();
            List<String> pkColumns = new ArrayList<>();
            while (rsPk.next()) {
                String schema = rsPk.getString("TABLE_SCHEM");
                if (user.equalsIgnoreCase(schema)) {
                    String columnName = rsPk.getString("COLUMN_NAME");
                    pkColumns.add(isMixedCase ? columnName : columnName.toLowerCase());
                } else if ("PUBLIC".equalsIgnoreCase(schema)) {
                    String columnName = rsPk.getString("COLUMN_NAME");
                    pkColumns.add(isMixedCase ? columnName : columnName.toLowerCase());
                }
            }
            return pkColumns;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Snapshot fetch(String table) {
        return fetch(table, null);
    }

    public ChangeSet delta(Snapshot snapshot, List<String> ignoreColumns) {
        Snapshot current = fetch(snapshot.getTable(), snapshot.getWhere());
        return snapshot.delta(current, ignoreColumns);
    }

    public boolean existsOrCreate(Snapshot snapshot) {
        try {
            PreparedStatement stmt = conn.prepareStatement("select * from " + snapshot.getTable() + " where 1 = 2");
            return stmt.execute();
        } catch (SQLException e) {
            try {
                String sql = "create table " + snapshot.getTable() + " (" + snapshot.columns().map(c -> c + " " + (snapshot.getColumnTypes().get(c).getSql())).collect(Collectors.joining(", ")) + ", primary key (" + snapshot.pkColumns().collect(Collectors.joining(", ")) + "))";
                return conn.prepareStatement(sql).execute();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public Snapshot fetch(String table, String whereStmt) {
        try {
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
                            record[i - 1] = String.valueOf(rs.getShort(i));
                            break;
                        case TINYINT:
                        case INTEGER:
                            record[i - 1] = String.valueOf(rs.getInt(i));
                            break;
                        case BIGINT:
                            record[i - 1] = String.valueOf(rs.getLong(i));
                            break;
                        case NUMERIC:
                        case DECIMAL:
                            record[i - 1] = String.valueOf(rs.getBigDecimal(i));
                            break;
                        case FLOAT:
                            record[i - 1] = String.valueOf(rs.getFloat(i));
                            break;
                        case REAL:
                        case DOUBLE:
                            record[i - 1] = String.valueOf(rs.getDouble(i));
                            break;
                        case BOOLEAN:
                            record[i - 1] = String.valueOf(rs.getBoolean(i));
                            break;
                        case NULL:
                            record[i - 1] = null;
                            break;
                        case DATE:
                            java.sql.Date date = rs.getDate(i);
                            record[i - 1] = date == null ? null : SQL_DATE_FORMAT.format(date);
                            break;
                        case TIME:
                            Time time = rs.getTime(i);
                            record[i - 1] = time == null ? null : time.toLocalTime().toString();
                            break;
                        case TIMESTAMP:
                            Timestamp timestamp = rs.getTimestamp(i);
                            record[i - 1] = timestamp == null ? null : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp.getTime()));
                            break;
                        case TIME_WITH_TIMEZONE:
                        case TIMESTAMP_WITH_TIMEZONE:
                        case BINARY:
                        case BIT:
                            break;
                        case NCLOB:
                        case CLOB:
                            Clob clob = rs.getClob(i);
                            record[i - 1] = clob == null ? null : clob.getSubString(1, (int) clob.length());
                            break;
                        case BLOB:
                            Blob blob = rs.getBlob(i);
                            record[i - 1] = blob == null ? null : new String(Base64.getEncoder().encode(blob.getBytes(0L, (int) blob.length())));
                            break;
                        default:
                            record[i - 1] = rs.getString(i);
                            break;
                    }
                }
                snapshot.addRecord(record);
            }
            return snapshot;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void apply(ChangeSet changes, boolean commit) {
        changes.applyInsert(conn);
        changes.applyUpdate(conn);
        changes.applyDelete(conn);
        if (commit) execute("commit;");

        for (String undoStmt : changes.sqlUndoStmt())
            output.user(undoStmt);
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

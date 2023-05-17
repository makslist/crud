package de.crud;

import oracle.jdbc.pool.OracleDataSource;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

import static java.sql.Types.*;

public class Crud {

    public static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static OutPut output = OutPut.getInstance();

    public static Crud connectHSQL() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            return new Crud(DriverManager.getConnection("jdbc:hsqldb:data/tutorial", "sa", ""));
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
            return new Crud(DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "/" + serviceName, user, password));
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
            return new Crud(DriverManager.getConnection("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1", "sa", "sa"));
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
            return new Crud(ods.getConnection());
        } catch (SQLException e) {
            output.error("Connection unsuccessful: " + e.getMessage());
            System.exit(1);
        }
        return null;
    }

    private final Connection conn;
    private final boolean isMixedCase;

    private Crud(Connection conn) throws SQLException {
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
                if ("xx".equalsIgnoreCase(schema)) {
                    String columnName = rsPk.getString("COLUMN_NAME");
                    pkColumns.add(isMixedCase ? columnName : columnName.toLowerCase());
                }
            }
            return pkColumns;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Snapshot fetchTable(String table) {
        return fetch(table, null);
    }

    public Snapshot fetch(String table, String whereStmt) {
        try {
            PreparedStatement stmt = conn.prepareStatement("select * from " + table + (whereStmt != null ? " where " + whereStmt : ""));
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            Map<String, Integer> columnTypes = new HashMap<>();
            String[] columns = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                String columnName = isMixedCase ? rsmd.getColumnName(i) : rsmd.getColumnName(i).toLowerCase();
                columnTypes.put(columnName, rsmd.getColumnType(i));
                columns[i - 1] = columnName;
            }

            Snapshot snapshot = new Snapshot(table, columns, columnTypes, descPkOf(table), whereStmt, new ArrayList<>());

            while (rs.next()) {
                String[] record = new String[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    record[i - 1] = switch (rsmd.getColumnType(i)) {
                        case SMALLINT -> String.valueOf(rs.getShort(i));
                        case TINYINT, INTEGER -> String.valueOf(rs.getInt(i));
                        case BIGINT -> String.valueOf(rs.getLong(i));
                        case NUMERIC, DECIMAL -> String.valueOf(rs.getBigDecimal(i));
                        case FLOAT -> String.valueOf(rs.getFloat(i));
                        case REAL, DOUBLE -> String.valueOf(rs.getDouble(i));
                        case BOOLEAN -> String.valueOf(rs.getBoolean(i));
                        case NULL -> null;
                        case DATE -> SQL_DATE_FORMAT.format(rs.getDate(i));
                        case TIME -> rs.getTime(i).toLocalTime().toString();
                        case TIMESTAMP ->
                            new SimpleDateFormat("yyyyMMdd").format(new Date(rs.getTimestamp(i).getTime()));
                        case TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, BINARY, BIT, ROWID -> rs.getString(i);
                        case CLOB, NCLOB -> rs.getClob(i).toString();
                        case BLOB ->
                            new String(Base64.getEncoder().encode(rs.getBlob(i).getBytes(0L, (int) rs.getBlob(i).length())));
                        default -> rs.getString(i);
                    };
                }
                snapshot.addRecord(record);
            }
            return snapshot;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void apply(ChangeSet changes) {
        changes.applyInsert(conn);
        changes.applyUpdate(conn);
        changes.applyDelete(conn);
    }

    public void write(ChangeSet changes, OutputStream out) {
        OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(out));
        try {
            for (String stmt : changes.sqlApplyStmt())
                writer.write(stmt + System.lineSeparator());
            writer.close();
        } catch (IOException e) {
            output.error("Failed writing: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Snapshot read(File file) {
        return Snapshot.read(file);
    }

}

package de.crud;

import javax.sql.rowset.serial.*;
import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static java.sql.Types.*;

public class ChangeSet {

    private final Snapshot reference;
    private final Snapshot target;

    private final List<Snapshot.Key> deleteKeys;
    private final List<Snapshot.Key> updateKeys;
    private final List<Snapshot.Key> insertKeys;

    private final OutPut output = OutPut.create(null);

    public ChangeSet(Snapshot reference, Snapshot target, List<Snapshot.Key> insertKeys, List<Snapshot.Key> updateKeys, List<Snapshot.Key> deleteKeys) {
        this.reference = reference;
        this.target = target;

        this.insertKeys = insertKeys;
        this.updateKeys = updateKeys;
        this.deleteKeys = deleteKeys;
    }

    public Snapshot getReference() {
        return reference;
    }

    public Snapshot getTarget() {
        return target;
    }

    public List<Snapshot.Record> deleteRecs() {
        return deleteKeys.stream().map(target::getRecord).collect(Collectors.toList());
    }

    public List<Snapshot.Record> updateRecs() {
        return updateKeys.stream().map(reference::getRecord).collect(Collectors.toList());
    }

    public List<Snapshot.Record> insertRecs() {
        return insertKeys.stream().map(reference::getRecord).collect(Collectors.toList());
    }

    public boolean isEmpty() {
        return deleteKeys.isEmpty() && updateKeys.isEmpty() && insertKeys.isEmpty();
    }

    public void displayDiff() {
        Map<String, Snapshot.SqlType> colTypes = getReference().getColumnTypes();

        String[] columnNames = getReference().columns().toArray(String[]::new);
        String recordFormatter = Arrays.stream(columnNames).map(n -> "%" + (alignRight(colTypes.get(n).type) ? "-" : "") + 12 + "s").collect(Collectors.joining(" | "));
        String[] keyColumnNames = getReference().pkColumns().toArray(String[]::new);
        String keyFormatter = Arrays.stream(keyColumnNames).map(n -> "%" + (alignRight(colTypes.get(n).type) ? "-" : "") + 12 + "s").collect(Collectors.joining(" | "));

        if (!insertKeys.isEmpty()) {
            output.user("New Records:");
            output.user(String.format(recordFormatter, (Object[]) columnNames));
            insertRecs().stream().map(r -> r.columns().toArray(String[]::new)).forEach(c -> output.user(String.format(recordFormatter, (Object[]) c)));
        }
        if (!deleteKeys.isEmpty()) {
            output.user("\nDelete Records:");
            output.user(String.format(keyFormatter, (Object[]) keyColumnNames));
            deleteKeys.stream().map(k -> k.columns().toArray(String[]::new)).forEach(c -> output.user(String.format(keyFormatter, (Object[]) c)));
        }
        if (!updateKeys.isEmpty()) {
            output.user("\nUpdated Records:");
            output.user(String.format(recordFormatter, (Object[]) columnNames));
            updateKeys.forEach(k -> {
                output.user(String.format(recordFormatter, (Object[]) reference.getRecord(k).columns().toArray(String[]::new)));
                output.user(String.format(recordFormatter, (Object[]) target.getRecord(k).columns().toArray(String[]::new)) + "\n");
            });
        }
    }

    public void applyInsert(Connection conn) {
        Snapshot ref = getReference();
        List<String> columns = ref.columns().collect(Collectors.toList());
        String cols = columns.stream().collect(Collectors.joining(", ", " (", ")"));
        String values = columns.stream().map(c -> "?").collect(Collectors.joining(", ", " values (", ")"));
        String sql = "insert into " + ref.getTable() + cols + values;

        for (Snapshot.Record rec : insertRecs()) {
            try {
                PreparedStatement stmt = conn.prepareStatement(sql);
                int i = 1;
                for (String col : columns)
                    bindVar(stmt, rec.columnType(col), i++, rec.column(col));

                output.info(stmt.toString());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void applyUpdate(Connection conn) {
        Snapshot ref = getReference();
        String set = ref.nonPkColumns().map(s -> s + " = ?").collect(Collectors.joining(", ", " set ", ""));
        String where = ref.getPk().columns().map(c -> c + " = ?").collect(Collectors.joining(" and ", " where ", ""));
        String sql = "update " + ref.getTable() + set + where;

        List<String> nonPkColumns = ref.nonPkColumns().collect(Collectors.toList());
        List<String> pkColumns = ref.pkColumns().collect(Collectors.toList());
        for (Snapshot.Record rec : updateRecs()) {
            try {
                PreparedStatement stmt = conn.prepareStatement(sql);
                int i = 1;
                for (String col : nonPkColumns)
                    bindVar(stmt, rec.columnType(col), i++, rec.column(col));
                for (String col : pkColumns)
                    bindVar(stmt, rec.columnType(col), i++, rec.column(col));

                output.info(stmt.toString());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void applyDelete(Connection conn) {
        Snapshot ref = getReference();
        String where = ref.getPk().columns().map(c -> c + " = ?").collect(Collectors.joining(" and ", " where ", ""));
        String sql = "delete " + ref.getTable() + where;

        List<String> pkColumns = ref.pkColumns().collect(Collectors.toList());
        for (Snapshot.Record rec : deleteRecs()) {
            try {
                PreparedStatement stmt = conn.prepareStatement(sql);
                int i = 1;
                for (String col : pkColumns)
                    bindVar(stmt, rec.columnType(col), i++, rec.column(col));

                output.info(stmt.toString());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean alignRight(int type) {
        switch (type) {
            case SMALLINT:
            case TINYINT:
            case INTEGER:
            case BIGINT:
            case NUMERIC:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case BOOLEAN:
                return false;
            default:
                return true;
        }
    }

    private void bindVar(PreparedStatement stmt, int type, int index, String value) throws SQLException {
        switch (type) {
            case SMALLINT:
                stmt.setShort(index, Short.parseShort(value));
                break;
            case TINYINT:
            case INTEGER:
                stmt.setInt(index, Integer.parseInt(value));
                break;
            case BIGINT:
                stmt.setLong(index, Long.parseLong(value));
                break;
            case NUMERIC:
            case DECIMAL:
                stmt.setBigDecimal(index, new BigDecimal(value));
                break;
            case FLOAT:
                stmt.setFloat(index, Float.parseFloat(value));
                break;
            case REAL:
            case DOUBLE:
                stmt.setDouble(index, Double.parseDouble(value));
                break;
            case BOOLEAN:
                stmt.setBoolean(index, Boolean.parseBoolean(value));
                break;
            case DATE:
                stmt.setDate(index, java.sql.Date.valueOf(value));
                break;
            case TIME:
                stmt.setTime(index, Time.valueOf(value));
                break;
            case TIMESTAMP:
                stmt.setTimestamp(index, Timestamp.valueOf(value));
                break;
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
            case BINARY:
            case BIT:
            case NULL:
            case OTHER:
            case REF:
            case ROWID:
            case SQLXML:
            case VARBINARY:
            case NCLOB:
            case CLOB:
                stmt.setClob(index, new SerialClob(value.toCharArray()));
                break;
            case BLOB:
                stmt.setBlob(index, new SerialBlob(Base64.getDecoder().decode(value.getBytes())));
                break;
            default:
                stmt.setString(index, value);
                break;
        }
    }

    public List<String> sqlApplyStmt() {
        List<String> stmts = new ArrayList<>();
        stmts.addAll(deleteSqlStmt(getTarget(), deleteRecs()));
        stmts.addAll(insertSqlStmt(getReference(), insertRecs()));
        stmts.addAll(updateSqlStmt(getReference(), updateRecs()));
        return stmts;
    }

    public List<String> sqlUndoStmt() {
        List<String> stmts = new ArrayList<>();
        stmts.addAll(deleteSqlStmt(getTarget(), insertRecs()));
        stmts.addAll(insertSqlStmt(getReference(), deleteRecs()));
        stmts.addAll(updateSqlStmt(getTarget(), updateRecs()));
        return stmts;
    }

    private List<String> deleteSqlStmt(Snapshot snpSht, List<Snapshot.Record> records) {
        String sql = "delete " + snpSht.getTable();
        List<String> stmts = new ArrayList<>();
        for (Snapshot.Record rec : records) {
            String delete = sql + snpSht.getPk().columns().map(c -> c + " = " + rec.column(c)).collect(Collectors.joining(" and ", " where ", "")) + ";";
            stmts.add(delete);
        }
        return stmts;
    }

    private List<String> insertSqlStmt(Snapshot snpSht, List<Snapshot.Record> records) {
        String sql = "insert into " + snpSht.getTable();
        List<String> stmts = new ArrayList<>();
        List<String> columns = snpSht.columns().collect(Collectors.toList());
        for (Snapshot.Record rec : records) {
            String cols = columns.stream().collect(Collectors.joining(", ", " (", ")"));
            String values = columns.stream().map(c -> formatSqlDataType(rec.columnType(c), rec.column(c))).collect(Collectors.joining(", ", " values (", ")"));
            stmts.add(sql + cols + values + ";");
        }
        return stmts;
    }

    private List<String> updateSqlStmt(Snapshot snpSht, List<Snapshot.Record> records) {
        String sql = "update " + snpSht.getTable();
        List<String> stmts = new ArrayList<>();
        for (Snapshot.Record rec : records) {
            String set = snpSht.nonPkColumns().map(c -> c + " = " + formatSqlDataType(rec.columnType(c), rec.column(c))).collect(Collectors.joining(", ", " set ", ""));
            String where = snpSht.getPk().columns().map(c -> c + " = " + rec.column(c)).collect(Collectors.joining(" and ", " where ", ""));
            stmts.add(sql + set + where + ";");
        }
        return stmts;
    }

    private String formatSqlDataType(int columnType, String value) {
        switch (columnType) {
            case DATE:
                return "DATE" + "'" + value + "'";
            case TIME:
                return "TIME" + "'" + value + "'";
            case TIMESTAMP:
                return "TIMESTAMP" + "'" + value + "'";
            case SMALLINT:
            case TINYINT:
            case INTEGER:
            case BIGINT:
            case NUMERIC:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case BOOLEAN:
                return value;
            case NULL:
                return null;
            case BLOB:
                return new BigInteger(Base64.getDecoder().decode(value.getBytes())).toString(16);
            case NCLOB:
            case CLOB:
            default:
                return "'" + value + "'";
        }
    }

}

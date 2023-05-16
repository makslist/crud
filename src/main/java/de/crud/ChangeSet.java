package de.crud;

import javax.sql.rowset.serial.SerialBlob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.*;
import java.util.stream.Collectors;

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
        Map<String, Integer> colTypes = getReference().getColumnTypes();

        String[] columnNames = getReference().columns().toArray(String[]::new);
        String recordFormatter = Arrays.stream(columnNames).map(n -> "%" + (alignRight(colTypes.get(n)) ? "-" : "") + 12 + "s").collect(Collectors.joining(" | "));
        String[] keyColumnNames = getReference().pkColumns().toArray(String[]::new);
        String keyFormatter = Arrays.stream(keyColumnNames).map(n -> "%" + (alignRight(colTypes.get(n)) ? "-" : "") + 12 + "s").collect(Collectors.joining(" | "));

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
                output.user((char) 27 + "[31m" + String.format(recordFormatter, (Object[]) target.getRecord(k).columns().toArray(String[]::new)));
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
        return switch (type) {
            case SMALLINT, TINYINT, INTEGER, BIGINT, NUMERIC, DECIMAL, FLOAT, REAL, DOUBLE, BOOLEAN -> false;
            default -> true;
        };
    }


    private void bindVar(PreparedStatement stmt, int type, int index, String value) throws SQLException {
        switch (type) {
            case SMALLINT -> stmt.setShort(index, Short.parseShort(value));
            case TINYINT, INTEGER -> stmt.setInt(index, Integer.parseInt(value));
            case BIGINT -> stmt.setLong(index, Long.parseLong(value));
            case NUMERIC, DECIMAL -> stmt.setBigDecimal(index, new BigDecimal(value));
            case FLOAT -> stmt.setFloat(index, Float.parseFloat(value));
            case REAL, DOUBLE -> stmt.setDouble(index, Double.parseDouble(value));
            case BOOLEAN -> stmt.setBoolean(index, Boolean.parseBoolean(value));
            case DATE -> stmt.setDate(index, java.sql.Date.valueOf(value));
            case TIME -> stmt.setTime(index, Time.valueOf(value));
            case TIMESTAMP, TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE, BINARY, BIT, NCLOB, NULL, OTHER, REF, ROWID, SQLXML, VARBINARY, CLOB ->
                    stmt.setClob(index, new javax.sql.rowset.serial.SerialClob(value.toCharArray()));
            case BLOB -> stmt.setBlob(index, new SerialBlob(Base64.getDecoder().decode(value.getBytes())));
            default -> stmt.setString(index, value);
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
        return switch (columnType) {
            case DATE -> "DATE" + "'" + value + "'";
            case TIME -> "TIME" + "'" + value + "'";
            case TIMESTAMP -> "TIMESTAMP" + "'" + value + "'";
            case SMALLINT, TINYINT, INTEGER, BIGINT, NUMERIC, DECIMAL, FLOAT, REAL, DOUBLE, BOOLEAN -> value;
            case NULL -> null;
            case NCLOB, CLOB -> value.replaceFirst("clob[0-9]+: ", "");
            case BLOB -> new BigInteger(Base64.getDecoder().decode(value.getBytes())).toString(16);
            default -> "'" + value + "'";
        };
    }

}

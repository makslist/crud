package de.crud;

import com.fasterxml.jackson.annotation.*;

import java.util.*;
import java.util.stream.*;

import static java.sql.Types.*;

public class TableMeta {

    String name = null;
    String remarks = null;
    List<Column> columns = null;
    PrimaryKey primaryKey = null;
    List<ForeignKey> foreignKeys = null;

    protected Map<String, Integer> columnIndex = null;

    public TableMeta() {
    }

    public TableMeta(String name, String remarks, List<TableMeta.Column> columns, PrimaryKey primaryKey, List<ForeignKey> foreignKeys) {
        this.name = name;
        this.remarks = remarks;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.primaryKey.setBackref(this);
        this.foreignKeys = foreignKeys;

        columnIndex = columns.stream().collect(Collectors.toMap(c -> c.name, c -> c.position - 1));
    }

    public String getName() {
        return name;
    }

    public String getRemarks() {
        return remarks;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        this.primaryKey.setBackref(this);
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    @JsonIgnore
    public Stream<Column> getPkColumns() {
        return primaryKey.columnNames.stream().flatMap(n -> columns.stream().filter(c -> n.equals(c.name)));
    }

    public static class Column {

        int position;
        String name = null;
        String remarks = null;
        int datatype;
        int columnSize;
        int decimalDigits;
        boolean isNullable;
        boolean isAutoIncrement;
        String defaultValue = null;

        public Column() {
        }

        public Column(int position,
                      String columnName,
                      String remarks,
                      int datatype,
                      int columnSize,
                      int decimalDigits,
                      boolean isNullable,
                      boolean isAutoIncrement,
                      String defaultValue) {
            this.position = position;
            this.name = columnName;
            this.remarks = remarks;
            this.datatype = datatype;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.isNullable = isNullable;
            this.isAutoIncrement = isAutoIncrement;
            this.defaultValue = defaultValue;
        }

        public int getPosition() {
            return position;
        }

        public String getName() {
            return name;
        }

        public String getRemarks() {
            return remarks;
        }

        public int getDatatype() {
            return datatype;
        }

        public int getColumnSize() {
            return columnSize;
        }

        public int getDecimalDigits() {
            return decimalDigits;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public boolean isAutoIncrement() {
            return isAutoIncrement;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Column))
                return false;
            Column column = (Column) obj;
            return name.equals(column.name) && position == column.position;
        }

        @JsonIgnore
        public String getTypeSql() {
            switch (datatype) {
                case BIT:
                    return "bit";
                case TINYINT:
                    return "tinyint";
                case SMALLINT:
                    return "smallint";
                case INTEGER:
                    return "integer";
                case BIGINT:
                    return "bigint";
                case FLOAT:
                    return "float";
                case REAL:
                    return "real";
                case DOUBLE:
                    return "double";
                case NUMERIC:
                    return String.format("numeric%s", columnSize == 0 ? "" : String.format(" (%d%s)", columnSize, (decimalDigits != 0 && decimalDigits != -127) ? ", " + decimalDigits : ""));
                case DECIMAL:
                    return "decimal";
                case CHAR:
                    return "char";
                case VARCHAR:
                    return "varchar(" + columnSize + ")";
                case LONGVARCHAR:
                    return "longvarchar";
                case DATE:
                    return "date";
                case TIME:
                    return "time";
                case TIMESTAMP:
                    return "timestamp";
                case BINARY:
                    return "binary";
                case VARBINARY:
                    return "varbinary";
                case LONGVARBINARY:
                    return "longvarbinary";
                case NULL:
                    return "null";
                case OTHER:
                    return "other";
                case JAVA_OBJECT:
                    return "java_object";
                case DISTINCT:
                    return "distinct";
                case STRUCT:
                    return "struct";
                case ARRAY:
                    return "array";
                case BLOB:
                    return "blob";
                case CLOB:
                    return "clob";
                case REF:
                    return "ref";
                case DATALINK:
                    return "datalink";
                case BOOLEAN:
                    return "boolean";
                case ROWID:
                    return "rowid";
                case NCHAR:
                    return "nchar";
                case NVARCHAR:
                    return "nvarchar";
                case LONGNVARCHAR:
                    return "longnvarchar";
                case NCLOB:
                    return "nclob";
                case SQLXML:
                    return "sqlxml";
                case REF_CURSOR:
                    return "ref_cursor";
                case TIME_WITH_TIMEZONE:
                    return "time_with_timezone";
                case TIMESTAMP_WITH_TIMEZONE:
                    return "timestamp_with_timezone";
                default:
                    throw new IllegalStateException("Unexpected value: " + datatype);
            }
        }
    }

    public static class PrimaryKey {
        TableMeta table;
        String name = null;
        int columnCount;
        private int[] pkIndices;
        List<String> columnNames;

        public PrimaryKey() {
        }

        public PrimaryKey(String name, List<String> columnNames) {
            this.name = name;
            this.columnNames = columnNames;
            this.columnCount = columnNames.size();
        }

        public void setBackref(TableMeta table) {
            this.table = table;
            this.pkIndices = table.columns.stream().filter(c -> columnNames.contains(c.name)).mapToInt(c -> c.position - 1).toArray();
        }

        public String getName() {
            return name;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            this.columnCount = columnNames.size();
        }

        public Stream<Column> columns() {
            return columnNames.stream().flatMap(n -> table.columns.stream().filter(c -> c.name.equals(n)));
        }

        @JsonIgnore
        public int[] getPkIndices() {
            return pkIndices;
        }

    }

    public static class ForeignKey {
        final String pkTableName = null;
        final String fkTableName = null;
        final String pkColumnName = null;
        final String fkColumnName = null;

        public String getPkTableName() {
            return pkTableName;
        }

        public String getFkTableName() {
            return fkTableName;
        }

        public String getPkColumnName() {
            return pkColumnName;
        }

        public String getFkColumnName() {
            return fkColumnName;
        }

    }

    public String createSql() {
        return "create table " + name + " (" + columns.stream().map(c -> c.name + " " + c.getTypeSql()).
                collect(Collectors.joining(", ")) + ", primary key (" + String.join(", ", primaryKey.columnNames) + "))";
    }

}

package org.makslist.dbd;

import com.fasterxml.jackson.annotation.*;

import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARCHAR;

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
        if (this.primaryKey != null)
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
            String format;
            switch (datatype) {
                case NUMERIC:
                    format = String.format("numeric%s", columnSize == 0 ? "" : String.format(" (%d%s)", columnSize, (decimalDigits != 0 && decimalDigits != -127) ? ", " + decimalDigits : ""));
                    break;
                case VARCHAR:
                    format = "(" + columnSize + ")";
                    break;
                default:
                    format = "";
            }
            return JDBCType.valueOf(datatype).getName() + format;
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
        String name;
        List<ColumnMapping> mappings;

        public ForeignKey() {
        }

        public ForeignKey(String name, List<ColumnMapping> mappings) {
            this.name = name;
            this.mappings = mappings;
        }

        public String getName() {
            return name;
        }

        public List<ColumnMapping> getMappings() {
            return mappings;
        }

        public void setMappings(List<ColumnMapping> mappings) {
            this.mappings = mappings;
        }

        public static class ColumnMapping {

            String pkTableName;
            String fkTableName;
            String pkColumnName;
            String fkColumnName;

            public ColumnMapping() {
            }

            public ColumnMapping(String pkTableName, String pkColumnName, String fkTableName, String fkColumnName) {
                this.pkTableName = pkTableName;
                this.pkColumnName = pkColumnName;
                this.fkTableName = fkTableName;
                this.fkColumnName = fkColumnName;
            }

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

    }

    public String createSql() {
        return "create table " + name + " (" + columns.stream().map(c -> c.name + " " + c.getTypeSql()).
                collect(Collectors.joining(", ")) + ", primary key (" + String.join(", ", primaryKey.columnNames) + "))";
    }

}

package de.crud;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

import java.io.*;
import java.math.*;
import java.nio.file.*;
import java.text.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.sql.Types.*;

public class Snapshot {

    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String WHERE = "where";
    public static final String COLUMN_TYPES = "columnTypes";
    public static final String PRIMARY_KEY = "primaryKey";
    public static final String RECORDS = "records";

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final ObjectWriter OBJECT_WRITER;

    private static final DecimalFormat DECIMAL_FORMAT;

    static {
        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
        OBJECT_WRITER = MAPPER.writer(prettyPrinter);

        DECIMAL_FORMAT = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        DECIMAL_FORMAT.setMaximumFractionDigits(340);
    }

    public static Snapshot read(File file) throws IOException {
        ObjectReader reader = MAPPER.reader();
        return reader.readValue(file, Snapshot.class);
    }

    private String table;
    private String[] columns;
    private Map<String, Integer> columnIndex;
    private Map<String, SqlType> columnTypes;
    private Key pk;
    private List<Integer> pkIndices;
    private String where;
    private List<Record> records = new ArrayList<>();
    private final Map<Key, Record> index = new HashMap<>();

    public Snapshot() {
    }

    public Snapshot(String table, String[] columns, Map<String, SqlType> columnTypes, List<String> pkColumns, String where) {
        this.table = table;
        this.columns = columns;
        this.columnIndex = columnIndex();
        this.columnTypes = columnTypes;
        this.pk = new Key(pkColumns.stream().sorted((k1, k2) -> columnIndex().get(k1) - columnIndex().get(k2)).collect(Collectors.toList()));
        this.pkIndices = pk.columns().map(columnIndex()::get).collect(Collectors.toList());
        this.where = where;
    }

    private Map<String, Integer> columnIndex() {
        if (this.columnIndex == null)
            this.columnIndex = IntStream.rangeClosed(0, this.columns.length - 1).boxed().collect(Collectors.toMap(i -> this.columns[i], Function.identity()));
        return this.columnIndex;
    }

    @JsonProperty(COLUMNS)
    public void setColumns(String[] columns) {
        this.columns = columns;
        if (this.pk != null && this.pkIndices == null)
            this.pk = new Key(this.pk.columns().sorted((k1, k2) -> columnIndex().get(k1) - columnIndex().get(k2)).toArray(String[]::new));
    }

    public Stream<String> columns() {
        return Arrays.stream(columns);
    }

    public List<Integer> pkIndex() {
        if (this.pkIndices == null)
            this.pkIndices = pk.columns().map(columnIndex()::get).collect(Collectors.toList());
        return this.pkIndices;
    }

    public Stream<Key> keys() {
        return records.stream().map(Record::key);
    }

    public boolean containedInIndex(Key key) {
        return index.containsKey(key);
    }

    public Record getRecord(Key key) {
        return index.get(key);
    }

    public String getTable() {
        return table;
    }

    public String getWhere() {
        return where;
    }

    @JsonProperty(WHERE)
    public void setWhere(String where) {
        this.where = where;
    }

    public Map<String, SqlType> getColumnTypes() {
        return this.columnTypes;
    }

    @JsonProperty(COLUMN_TYPES)
    public void setColumnTypes(Map<String, SqlType> columnTypes) {
        this.columnTypes = new HashMap<>();
        this.columnTypes.putAll(columnTypes);
    }

    public Key getPk() {
        return pk;
    }

    public Stream<String> pkColumns() {
        return IntStream.range(0, columns.length).filter(i -> pkIndex().contains(i)).mapToObj(i -> columns[i]);
    }

    public Stream<String> nonPkColumns() {
        return IntStream.range(0, columns.length).filter(i -> !pkIndex().contains(i)).mapToObj(i -> columns[i]);
    }

    @JsonProperty(PRIMARY_KEY)
    public void setPkColumns(String[] columns) {
        this.pk = new Key(columns);
        if (columnIndex() != null)
            this.pk = new Key(Stream.of(columns).sorted((k1, k2) -> columnIndex().get(k1) - columnIndex().get(k2)).toArray(String[]::new));
    }

    public List<Record> getRecords() {
        return records;
    }

    @JsonProperty(RECORDS)
    public void setRecords(Map<String, String>[] columns) {
        this.records = new ArrayList<>();
        Arrays.asList(columns).forEach(r -> {
            Record e = new Record(this, r.values().toArray(new String[0]));
            records.add(e);
            index.put(e.key(), e);
        });
    }

    public void addRecord(String[] record) {
        Record rec = new Record(this, record);
        if (columns.length != rec.size()) throw new RuntimeException("Column count is different.");

        records.add(rec);
        index.put(rec.key(), rec);
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public ChangeSet delta(Snapshot target, List<String> ignoreColumns) {
        if (!table.equalsIgnoreCase(target.table)) throw new RuntimeException("The tables have to have the same name.");
        if (!Arrays.equals(columns, target.columns)) {
            System.out.println("   The columns names and positions have to be identical.");
            System.out.println("   Reference order: " + Arrays.toString(columns));
            System.out.println("   Found: " + Arrays.toString(target.columns));
        }

        List<Snapshot.Key> deleteKeys = target.keys().filter(r -> !containedInIndex(r)).collect(Collectors.toList());
        boolean[] useColumn = useColumns(ignoreColumns);
        List<Snapshot.Key> updateKeys = keys().filter(target::containedInIndex).filter(key -> !getRecord(key).equals(target.getRecord(key), useColumn)).collect(Collectors.toList());
        List<Snapshot.Key> insertKeys = keys().filter(r -> !target.containedInIndex(r)).collect(Collectors.toList());
        return new ChangeSet(this, target, insertKeys, updateKeys, deleteKeys);
    }

    private boolean[] useColumns(List<String> ignoreColumns) {
        for (String ignoreColumn : ignoreColumns)
            if (pkColumns().anyMatch(ignoreColumn::equals))
                throw new RuntimeException("PrimaryKey columns can not be ignored.");

        if (!ignoreColumns.isEmpty()) {
            boolean[] useColumn = new boolean[columns.length];
            columns().forEach(c -> useColumn[columnIndex().get(c)] = !ignoreColumns.contains(c));
            return useColumn;
        } else return null;
    }

    public void export(List<String> groupBy, File path) throws IOException {
        if (!groupBy.stream().allMatch(g -> columns().anyMatch(g::equalsIgnoreCase)))
            throw new RuntimeException("Column does not exist in table.");

        Map<List<String>, List<Snapshot.Record>> groups = records.stream().collect(Collectors.groupingBy(rec -> groupBy.stream().map(rec::column).collect(Collectors.toList())));

        for (Map.Entry<List<String>, List<Snapshot.Record>> group : groups.entrySet()) {
            StringJoiner whereGrp = new StringJoiner(" and ");
            if (where != null) whereGrp.add(where);

            IntStream.range(0, group.getKey().size()).mapToObj(i -> groupBy.get(i) + " = " + group.getKey().get(i)).forEach(whereGrp::add);
            StringJoiner filename = new StringJoiner("_", "", ".snapshot").add(getTable()).add(String.join("_", group.getKey()));
            export(Files.newOutputStream(new File(path, filename.toString()).toPath()), whereGrp.toString(), group.getValue());
        }
    }

    public void export(OutputStream out) throws IOException {
        export(out, where, records);
    }

    private void export(OutputStream out, String whereStmt, List<Record> recs) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put(TABLE, this.table);
        columns().forEach(root.putArray(COLUMNS)::add);

        ObjectNode colTypes = MAPPER.createObjectNode();
        for (Map.Entry<String, SqlType> entry : this.columnTypes.entrySet()) {
            ObjectNode type = MAPPER.createObjectNode();
            type.put("type", entry.getValue().type);
            type.put("precision", entry.getValue().precision);
            type.put("scale", entry.getValue().scale);
            colTypes.putIfAbsent(entry.getKey(), type);
        }
        root.putIfAbsent(COLUMN_TYPES, colTypes);

        this.pk.columns().forEach(root.putArray(PRIMARY_KEY)::add);
        root.put(WHERE, whereStmt);

        ArrayNode rows = root.putArray(RECORDS);
        for (Record r : recs) {
            ObjectNode node = MAPPER.createObjectNode();
            for (int i = 0; i < columns.length; i++) {
                String value = r.columns[i];
                switch (this.columnTypes.get(columns[i]).type) {
                    case TINYINT:
                    case INTEGER:
                        node.put(columns[i], value != null ? Integer.valueOf(value) : null);
                        break;
                    case SMALLINT:
                        node.put(columns[i], value != null ? Short.valueOf(value) : null);
                        break;
                    case BIGINT:
                        node.put(columns[i], value != null ? new BigInteger(value) : null);
                        break;
                    case FLOAT:
                        node.put(columns[i], value != null ? Float.valueOf(value) : null);
                        break;
                    case REAL:
                    case DOUBLE:
                        node.put(columns[i], value != null ? Double.valueOf(value) : null);
                        break;
                    case BIT:
                    case NULL:
                    default:
                        node.put(columns[i], value);
                        break;
                }
            }
            rows.add(node);
        }

        OBJECT_WRITER.writeValue(out, root);
    }

    public static class SqlType {

        int type;
        int precision;

        int scale;

        public SqlType() {
        }

        public SqlType(int type, int precision, int scale) {
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public int getScale() {
            return scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
        }

        public String getSql() {
            switch (type) {
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
                    return String.format("numeric%s", precision == 0 ? "" : String.format(" (%d%s)", precision, (scale != 0 && scale != -127) ? ", " + scale : ""));
                case DECIMAL:
                    return "decimal";
                case CHAR:
                    return "char";
                case VARCHAR:
                    return "varchar(" + precision + ")";
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
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        }
    }

    public static class Key {

        private final String[] columns;

        public Key(List<String> columns) {
            this.columns = columns.toArray(new String[0]);
        }

        public Key(String[] columns) {
            this.columns = columns;
        }

        public Stream<String> columns() {
            return Arrays.stream(columns);
        }

        public String toString() {
            return Arrays.toString(columns);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Arrays.equals(columns, key.columns);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(columns);
        }

    }

    public static class Record {

        private final Snapshot snapshot;
        private final String[] columns;

        protected Record(Snapshot snapshot, String[] columns) {
            this.snapshot = snapshot;
            this.columns = columns;
        }

        public String column(String name) {
            return columns[snapshot.columnIndex().get(name)];
        }

        public Stream<String> columns() {
            return Arrays.stream(columns);
        }

        public Integer columnType(String name) {
            return snapshot.getColumnTypes().get(name).type;
        }

        public Key key() {
            return new Key(snapshot.pkIndex().stream().map(i -> columns[i]).collect(Collectors.toList()));
        }

        public boolean equals(Record comp, boolean[] useColumn) {
            for (int i = 0; i < columns.length; i++) {
                if (useColumn != null && !useColumn[i])
                    continue;
                if (!Objects.equals(columns[i], comp.columns[i]))
                    return false;
            }
            return true;
        }

        public int size() {
            return columns.length;
        }

        public String toString() {
            return Arrays.toString(columns);
        }

    }

}

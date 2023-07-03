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
import java.util.stream.*;

import static java.sql.Types.*;

public class Snapshot {

    public static final String TABLE = "table";
    public static final String WHERE = "where";
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

    private TableMeta table;

    private String where;
    private List<Record> records = new ArrayList<>();
    private final Map<Key, Record> index = new HashMap<>();

    public Snapshot() {
    }

    public Snapshot(TableMeta table, String where) {
        this.table = table;
        this.where = where;
    }

    public TableMeta getTable() {
        return table;
    }

    public String getTableName() {
        return table.name;
    }

    public Stream<String> columnNames() {
        return table.columns.stream().map(c -> c.name);
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

    public String getWhere() {
        return where;
    }

    @JsonProperty(WHERE)
    public void setWhere(String where) {
        this.where = where;
    }

    public Stream<String> pkColumns() {
        return table.primaryKey.columnNames.stream();
    }

    public Stream<String> nonPkColumns() {
        return table.columns.stream().map(c -> c.name).filter(c -> pkColumns().noneMatch(c::equals));
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
        if (table.columns.size() != record.length)
            throw new RuntimeException("Column count is different.");

        Record rec = new Record(this, record);
        records.add(rec);
        index.put(rec.key(), rec);
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public ChangeSet delta(Snapshot target, List<String> ignoreColumns) {
        if (!table.name.equalsIgnoreCase(target.table.name))
            throw new RuntimeException("The tables have to have the same name.");
        if (!table.columns.equals(target.table.columns)) {
            System.out.println("   The columns names and positions have to be identical.");
            System.out.println("   Reference order: " + table.columns.stream().map(c -> c.name).collect(Collectors.joining(", ")));
            System.out.println("   Found: " + target.table.columns.stream().map(c -> c.name).collect(Collectors.joining(", ")));
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
            boolean[] useColumn = new boolean[table.columns.size()];
            columnNames().forEach(c -> useColumn[table.columnIndex.get(c)] = !ignoreColumns.contains(c));
            return useColumn;
        } else return null;
    }

    public void export(List<String> groupBy, File path) throws IOException {
        if (!groupBy.stream().allMatch(g -> columnNames().anyMatch(g::equalsIgnoreCase)))
            throw new RuntimeException("Column does not exist in table.");

        Map<List<String>, List<Snapshot.Record>> groups = records.stream().collect(Collectors.groupingBy(rec -> groupBy.stream().map(rec::column).collect(Collectors.toList())));

        for (Map.Entry<List<String>, List<Snapshot.Record>> group : groups.entrySet()) {
            StringJoiner whereGrp = new StringJoiner(" and ");
            if (where != null) whereGrp.add(where);

            IntStream.range(0, group.getKey().size()).mapToObj(i -> groupBy.get(i) + " = " + group.getKey().get(i)).forEach(whereGrp::add);
            StringJoiner filename = new StringJoiner("_", "", ".snapshot").add(getTableName()).add(String.join("_", group.getKey()));
            export(Files.newOutputStream(new File(path, filename.toString()).toPath()), whereGrp.toString(), group.getValue());
        }
    }

    public void export(OutputStream out) throws IOException {
        export(out, where, records);
    }

    private void export(OutputStream out, String whereStmt, List<Record> recs) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.putPOJO(TABLE, this.table);
        root.put(WHERE, whereStmt);

        ArrayNode rows = root.putArray(RECORDS);
        for (Record r : recs) {
            ObjectNode node = MAPPER.createObjectNode();
            for (int i = 0; i < table.columns.size(); i++) {
                String value = r.columns[i];
                TableMeta.Column column = table.columns.get(i);
                switch (column.datatype) {
                    case TINYINT:
                    case INTEGER:
                        node.put(column.name, value != null ? Integer.valueOf(value) : null);
                        break;
                    case SMALLINT:
                        node.put(column.name, value != null ? Short.valueOf(value) : null);
                        break;
                    case BIGINT:
                        node.put(column.name, value != null ? new BigInteger(value) : null);
                        break;
                    case FLOAT:
                        node.put(column.name, value != null ? Float.valueOf(value) : null);
                        break;
                    case REAL:
                    case DOUBLE:
                        node.put(column.name, value != null ? Double.valueOf(value) : null);
                        break;
                    case BIT:
                    case NULL:
                    default:
                        node.put(column.name, value);
                        break;
                }
            }
            rows.add(node);
        }

        OBJECT_WRITER.writeValue(out, root);
    }

    public static class Key {

        private final String[] columns;

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
            return columns[snapshot.table.columnIndex.get(name)];
        }

        public Stream<String> columns() {
            return Arrays.stream(columns);
        }

        public int columnType(String name) {
            return snapshot.table.columns.get(snapshot.table.columnIndex.get(name)).datatype;
        }

        public Key key() {
            //TODO
            String[] keyElems = new String[snapshot.table.primaryKey.columnCount];
//            System.arraycopy(columns, 0, keyElems, 0, snapshot.table.primaryKeys.size());
//            return new Key(keyElems);

            int idx = 0;
            for (int i : snapshot.table.primaryKey.getPkIndices())
                keyElems[idx++] = columns[i];
            return new Key(keyElems);
        }

        public boolean equals(Record comp, boolean[] useColumn) {
            for (int i = 0; i < columns.length; i++)
                if ((useColumn == null || useColumn[i]) && !Objects.equals(columns[i], comp.columns[i]))
                    return false;

            return true;
        }

        public String toString() {
            return Arrays.toString(columns);
        }

    }

}

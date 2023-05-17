package de.crud;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.sql.Types.*;

public class Snapshot {

    public static final String TABLE = "table";
    public static final String COLUMNS = "columns";
    public static final String WHERE = "where";
    public static final String COLUMN_TYPES = "columnTypes";
    public static final String PRIMARY_KEY = "primaryKey";
    public static final String RECORDS = "records";

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final ObjectWriter objectWriter;

    static {
        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
        objectWriter = MAPPER.writer(prettyPrinter);
    }

    public static Snapshot read(File file) {
        ObjectReader reader = MAPPER.reader();
        try {
            return reader.readValue(file, Snapshot.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String table;
    private String[] columns;
    private Map<String, Integer> columnIndex;
    private Map<String, Integer> columnTypes;
    private Key pk;
    private List<Integer> pkIndices = new ArrayList<>();
    private String where;
    private List<Record> records = new ArrayList<>();
    private final Map<Key, Record> index = new HashMap<>();

    public Snapshot() {
    }

    public Snapshot(String table, String[] columns, Map<String, Integer> columnTypes, List<String> pkColumns, String where, List<String> ignoreColumns) {
        this.table = table;
        this.columns = columns;
        this.columnIndex = IntStream.rangeClosed(0, columns.length - 1).boxed().collect(Collectors.toMap(i -> columns[i], Function.identity()));
        this.columnTypes = columnTypes;
        this.pk = new Key(pkColumns);
        this.pkIndices = pkColumns.stream().map(columnIndex::get).collect(Collectors.toList());
        this.where = where;
    }

    public Stream<String> columns() {
        return Arrays.stream(columns);
    }

    public List<Integer> pkIndex() {
        return pkIndices;
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

    public Map<String, Integer> getColumnTypes() {
        return this.columnTypes;
    }

    @JsonProperty(COLUMN_TYPES)
    public void setColumnTypes(Map<String, Integer> columnTypes) {
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
    }

    public String getWhere() {
        return where;
    }

    public List<Record> getRecords() {
        return records;
    }

    @JsonProperty(RECORDS)
    public void setRecords(Map<String, String>[] columns) {
        this.records = new ArrayList<>();
        List.of(columns).forEach(r -> {
            Record e = new Record(this, r.values().toArray(new String[0]));
            records.add(e);
            index.put(e.key(), e);
        });
    }

    public void addRecord(String[] record) {
        Record rec = new Record(this, record);
        if (columnTypes.size() != rec.size()) throw new RuntimeException("Column count is different.");

        records.add(rec);
        index.put(rec.key(), rec);
    }

    public ChangeSet diff(Snapshot target) {
        return diff(target, Collections.emptyList());
    }

    public ChangeSet diff(Snapshot target, List<String> ignoreColumns) {
        if (!table.equals(target.table)) throw new RuntimeException("The tables have to have the same name.");
        if (!Arrays.equals(columns, target.columns))
            throw new RuntimeException("The columns name and position have to be identical.");

        List<Snapshot.Key> deleteKeys = target.keys().filter(r -> !containedInIndex(r)).collect(Collectors.toList());
        List<Snapshot.Key> updateKeys = keys().filter(target::containedInIndex).filter(key -> !getRecord(key).equals(target.getRecord(key), useColumns(ignoreColumns))).collect(Collectors.toList());
        List<Snapshot.Key> insertKeys = keys().filter(r -> !target.containedInIndex(r)).collect(Collectors.toList());
        return new ChangeSet(this, target, insertKeys, updateKeys, deleteKeys);
    }

    private boolean[] useColumns(List<String> ignoreColumns) {
        for (String ignoreColumn : ignoreColumns)
            if (pkColumns().anyMatch(ignoreColumn::equals))
                throw new RuntimeException("PrimaryKey columns can not be ignored.");

        if (!ignoreColumns.isEmpty()) {
            boolean[] useColumn = new boolean[columns.length];
            Arrays.fill(useColumn, true);
            ignoreColumns.forEach(c -> useColumn[columnIndex.get(c)] = false);
            return useColumn;
        } else return null;
    }

    public void export(List<String> groupBy, OutputStream out) {
        Map<List<String>, List<Snapshot.Record>> groups = records.stream().collect(Collectors.groupingBy(rec -> groupBy.stream().map(rec::column).collect(Collectors.toList())));

        for (Map.Entry<List<String>, List<Snapshot.Record>> group : groups.entrySet()) {
            StringJoiner whereGrp = new StringJoiner(" and ").add(where);
            IntStream.range(0, group.getKey().size()).mapToObj(i -> groupBy.get(i) + " = " + group.getKey().get(i)).forEach(whereGrp::add);
            try {
                export(new FileOutputStream("/home/maks/" + whereGrp + ".snapshot"), whereGrp.toString(), group.getValue()); //TODO
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void export(OutputStream out) {
        export(out, where, records);
    }

    private void export(OutputStream out, String whereStmt, List<Record> recs) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put(TABLE, this.table);
        Arrays.stream(columns).forEach(root.putArray(COLUMNS)::add);

        ObjectNode colTypes = MAPPER.createObjectNode();
        for (Map.Entry<String, Integer> entry : this.columnTypes.entrySet())
            colTypes.put(entry.getKey(), entry.getValue());
        root.putIfAbsent(COLUMN_TYPES, colTypes);

        Arrays.stream(this.pk.columns).forEach(root.putArray(PRIMARY_KEY)::add);
        root.put(WHERE, whereStmt);

        ArrayNode rows = root.putArray(RECORDS);
        for (Record r : recs) {
            ObjectNode node = MAPPER.createObjectNode();
            for (int i = 0; i < columns.length; i++) {
                if ("null".equals(r.columns[i]))
                    node.put(columns[i], (String) null);
                else {
                    switch (this.columnTypes.get(columns[i])) {
                        case BIT -> node.put(columns[i], String.valueOf(r.columns[i]));
                        case INTEGER, TINYINT -> node.put(columns[i], Integer.valueOf(r.columns[i]));
                        case SMALLINT -> node.put(columns[i], Short.valueOf(r.columns[i]));
                        case BIGINT -> node.put(columns[i], Long.valueOf(r.columns[i]));
                        case FLOAT -> node.put(columns[i], Float.valueOf(r.columns[i]));
                        case REAL, DOUBLE -> node.put(columns[i], Double.valueOf(r.columns[i]));
                        case NUMERIC -> node.put(columns[i], Long.parseLong(r.columns[i]));
                        case DECIMAL -> node.put(columns[i], BigDecimal.valueOf(Long.parseLong(r.columns[i])));
                        case NULL -> node.put(columns[i], (String) null);
                        default -> node.put(columns[i], r.columns[i]);
                    }
                }
            }
            rows.add(node);
        }

        try {
            objectWriter.writeValue(out, root);
        } catch (IOException e) {
            throw new RuntimeException(e);
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

        private final Snapshot entries;
        private final String[] columns;

        protected Record(Snapshot entries, String[] columns) {
            this.entries = entries;
            this.columns = columns;
        }

        public String column(String name) {
            return columns[entries.columnIndex.get(name)];
        }

        public Stream<String> columns() {
            return Arrays.stream(columns);
        }

        public Integer columnType(String name) {
            return entries.getColumnTypes().get(name);
        }

        public Key key() {
            return new Key(entries.pkIndices.stream().map(i -> columns[i]).collect(Collectors.toList()));
        }

        public String diff(Record record) {
            StringJoiner sj = new StringJoiner(", ");
            for (int i = 0; i < columns.length; i++)
                if (columns[i].equals(record.columns[i]))
                    sj.add(columns[i] + " (" + "\u001B31;1m" + record.columns[i] + ") ");
            return sj.toString();
        }

        public boolean equals(@NotNull Record comp, boolean[] useColumn) {
            if (useColumn != null) {
                for (int i = 0; i < useColumn.length; i++)
                    if (useColumn[i] && !Objects.equals(columns[i], comp.columns[i])) return false;
                return true;
            } else return Arrays.equals(columns, comp.columns);
        }

        public int size() {
            return columns.length;
        }

    }

}

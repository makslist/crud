import com.fasterxml.jackson.core.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class JsonExporter {

  private static final ObjectMapper mapper;
  private static final ObjectWriter objectWriter;

  static {
    mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
    objectWriter = mapper.writer(prettyPrinter);
  }

  private final String table;
  private final ObjectNode root;
  private final List<String> where;
  private final List<ObjectNode> rows = new ArrayList<>();

  public JsonExporter(String table, List<String> pkColumns, List<String> where) {
    this.table = table;
    this.where = where;
    root = mapper.createObjectNode();
    ArrayNode pk = root.putArray("primaryKeys");
    pkColumns.forEach(pk::add);
  }

  public ObjectNode createNode() {
    ObjectNode objectNode = mapper.createObjectNode();
    rows.add(objectNode);
    return objectNode;
  }

  public void write(File path, List<String> groupBy, Function<String, String> caseConvert) throws IOException {
    if (!path.isDirectory() && !path.mkdir()) return;

    Function<ObjectNode, List<String>> groupKey = n -> groupBy.stream().map(caseConvert).map(g -> n.get(g).asText()).collect(Collectors.toList());
    Map<List<String>, List<ObjectNode>> groups = rows.stream().collect(Collectors.groupingBy(groupKey));
    for (Map.Entry<List<String>, List<ObjectNode>> entry : groups.entrySet()) {
      List<String> whereAndGroup = new ArrayList<>(where);
      for (int i = 0; i < groupBy.size(); i++)
        whereAndGroup.add(groupBy.get(i) + " = '" + entry.getKey().get(i) + "'");
      ArrayNode records = root.putArray("records");
      entry.getValue().forEach(records::add);

      root.put("select", "select * from " + table + whereAndGroup.stream().collect(Collectors.joining(" and ", " where ", "")));

      String filename = table + "_" + String.join("_", entry.getKey());
      System.out.println("Writing: " + filename);
      objectWriter.writeValue(new File(path, filename.toLowerCase() + ".json"), root);
    }
  }

}

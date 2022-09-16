import com.fasterxml.jackson.databind.node.*;
import oracle.jdbc.pool.*;

import java.io.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.function.*;

public class Crud {

  public static final File BASE_PATH = new File("D:" + File.separator + "crud" + File.separator);

  public static Crud connectH2() {
    Crud crud = new Crud();
    try {
      crud.conn = DriverManager.getConnection("jdbc:h2:mem:myDb;DB_CLOSE_DELAY=-1", "sa", "sa");
    } catch (SQLException e) {
      System.out.println("Connection unsuccessful: " + e.getMessage());
      System.exit(1);
    }
    return crud;
  }

  public static Crud connectOracle(String hostname, int port, String serviceName, String user, String password) throws SQLException {
    Crud crud = new Crud();

    OracleDataSource ods = new OracleDataSource();
    ods.setURL("jdbc:oracle:thin:@//" + hostname + ":" + port + "/" + serviceName);
    ods.setUser(user);
    ods.setPassword(password);
    crud.conn = ods.getConnection();
    crud.conn.setAutoCommit(false);

    return crud;
  }

  private Connection conn;

  private Crud() {
  }

  private Function<String, String> getCaseConverter() {
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      boolean stroesUpperCase = metaData.storesUpperCaseIdentifiers();
      boolean storesLowerCase = metaData.storesLowerCaseIdentifiers();
      return s -> stroesUpperCase ? s.toUpperCase() : (storesLowerCase ? s.toLowerCase() : s);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void execute(String sql) throws SQLException {
    conn.prepareStatement(sql).execute();
  }

  public static void main(String[] args) {
    Crud crud = Crud.connectH2();
    try (crud.conn) {
      crud.execute("create table laeger (lager varchar(3), bez varchar(30), time_aen date, nr_lfd integer, time_neu time default CURRENT_TIME,PRIMARY KEY (lager, nr_lfd))");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test123', CURRENT_DATE, 1)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test456', CURRENT_DATE, 2)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test189', CURRENT_DATE, 3)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test289', CURRENT_DATE, 4)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test389', CURRENT_DATE, 5)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('645', 'test489', CURRENT_DATE, 6)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test589', CURRENT_DATE, 7)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test689', CURRENT_DATE, 8)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('787', 'test889', CURRENT_DATE, 9)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('445', 'test989', CURRENT_DATE, 10)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test789', CURRENT_DATE, 11)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test129', CURRENT_DATE, 12)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('234', 'test139', CURRENT_DATE, 13)");
      crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test149', CURRENT_DATE, 14)");

//      Crud crud = connectOracle("CUBE.tom", 1521, "XEPDB1", "system", "helas");

      List<String> where = List.of();
      List<String> pkColumns = crud.getPrimaryKey("laeger");
      String sql = "select * from " + "laeger" + (where.isEmpty() ? "" : " where " + String.join(" and ")) + (pkColumns.isEmpty() ? "" : " order by " + String.join(", ", pkColumns));
      System.out.println(sql);
      JsonExporter je = new JsonExporter("laeger", pkColumns, where);
      crud.fetchRows(sql, je);
      je.write(BASE_PATH, List.of("lager"), crud.getCaseConverter());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private List<String> getPrimaryKey(String table) throws SQLException {
    Function<String, String> caseConvert = getCaseConverter();
    ResultSet rsPk = conn.getMetaData().getPrimaryKeys(null, null, caseConvert.apply(table));
    List<String> pkColumns = new ArrayList<>();
    while (rsPk.next()) pkColumns.add(rsPk.getString(caseConvert.apply("COLUMN_NAME")));
    return pkColumns;
  }

  private void fetchRows(String sql, JsonExporter je) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setFetchSize(1000);
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columns = rsmd.getColumnCount();

    while (rs.next()) {
      ObjectNode node = je.createNode();
      for (int i = 1; i <= columns; i++) {
        String columnName = rsmd.getColumnName(i);
        int columnType = rsmd.getColumnType(i);
        switch (columnType) {
          case Types.SMALLINT:
            node.put(columnName, rs.getShort(i));
            break;
          case Types.TINYINT:
          case Types.INTEGER:
            node.put(columnName, rs.getInt(i));
            break;
          case Types.BIGINT:
            node.put(columnName, rs.getLong(i));
            break;
          case Types.NUMERIC:
          case Types.DECIMAL:
            node.put(columnName, rs.getBigDecimal(i));
            break;
          case Types.FLOAT:
            node.put(columnName, rs.getFloat(i));
            break;
          case Types.REAL:
          case Types.DOUBLE:
            node.put(columnName, rs.getDouble(i));
            break;
          case Types.BOOLEAN:
            node.put(columnName, rs.getBoolean(i));
            break;
          case Types.DATE:
            java.sql.Date sqlDate = rs.getDate(i);
            LocalDate date = sqlDate.toLocalDate();
            Instant instant = date.atStartOfDay(ZoneOffset.UTC).toInstant();
            node.put(columnName, instant.toString());
            break;
          case Types.TIME:
            java.sql.Time sqlTime = rs.getTime(i);
            LocalTime time = sqlTime.toLocalTime();
            node.put(columnName, time.toString());
            break;
          case Types.TIMESTAMP:
            break;//TODO
          case Types.TIME_WITH_TIMEZONE:
            break;//TODO
          case Types.TIMESTAMP_WITH_TIMEZONE:
            break;//TODO
          case Types.ARRAY:
            break;//TODO
          case Types.BINARY:
            break;//TODO
          case Types.BIT:
            break;//TODO
          case Types.DATALINK:
            break;//TODO
          case Types.DISTINCT:
            break;//TODO
          case Types.JAVA_OBJECT:
            break;//TODO
          case Types.NCLOB:
            break;//TODO
          case Types.NULL:
            break;//TODO
          case Types.OTHER:
            break;//TODO
          case Types.REF:
            break;//TODO
          case Types.REF_CURSOR:
            break;//TODO
          case Types.ROWID:
            break;//TODO
          case Types.SQLXML:
            break;//TODO
          case Types.STRUCT:
            break;//TODO
          case Types.VARBINARY:
            break;//TODO
          case Types.CLOB:
            node.put(columnName, rs.getClob(i).toString());
            break;
          case Types.BLOB:
            Blob blob = rs.getBlob(i);
            node.put(columnName, blob.getBytes(0L, (int) blob.length()));
            break;
          default:
            node.put(columnName, rs.getString(i));
        }
      }
    }
  }

}

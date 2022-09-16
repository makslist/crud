import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class TableExportTest {

  @BeforeEach
  void setUp() throws SQLException {
    Crud crud = new Crud();
    Connection conn = crud.connectH2();

    conn.prepareStatement("create table laeger (lager varchar(3), bez varchar(30), " + "time_aen date, nr_lfd " + "integer, time_neu time default CURRENT_TIME,PRIMARY KEY (lager, nr_lfd))").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test123', CURRENT_DATE, 1)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test456', CURRENT_DATE, 2)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test189', " + "CURRENT_DATE, 3)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test289', " + "CURRENT_DATE, 4)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test389', " + "CURRENT_DATE, 5)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('645', 'test489', " + "CURRENT_DATE, 6)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test589', " + "CURRENT_DATE, 7)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test689', " + "CURRENT_DATE, 8)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('787', 'test889', " + "CURRENT_DATE, 9)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('445', 'test989', " + "CURRENT_DATE, 10)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test789', " + "CURRENT_DATE, 11)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test129', " + "CURRENT_DATE, 12)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('234', 'test139', " + "CURRENT_DATE, 13)").execute();
    conn.prepareStatement("insert into laeger (lager, bez,time_aen, nr_lfd) values " + "('123', 'test149', " + "CURRENT_DATE, 14)").execute();
  }

}
package de.crud;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.List;

public class SnapshotExportTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table laeger (lager varchar(3), bez varchar(30), " + "time_aen date, nr_lfd " + "integer, time_neu time default CURRENT_TIME,PRIMARY KEY (lager, nr_lfd))");
    }

    @AfterEach
    void tearDown() {
        crud.execute("drop table laeger");
        crud.close();
    }

    @Test
    void descPkOfTable() {
        List<String> pk = crud.descPkOf("laeger");
        Assertions.assertEquals("lager", pk.get(0));
        Assertions.assertEquals("nr_lfd", pk.get(1));
    }

    @Test
    void fetchSnapshot() {
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('645', 'test489', CURRENT_DATE, 6)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test589', CURRENT_DATE, 7)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('787', 'test889', CURRENT_DATE, 9)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('445', 'test989', CURRENT_DATE, 10)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test789', CURRENT_DATE, 11)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('234', 'test139', CURRENT_DATE, 13)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test149', CURRENT_DATE, 14)");
        Snapshot rows = crud.fetch("laeger", "lager = '123'");
        Assertions.assertEquals(4, rows.getRecords().size());
        rows.export(OutputStream.nullOutputStream());
    }

    @Test
    void exportSnapshotPartition() {
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('645', 'test489', CURRENT_DATE, 6)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test789', CURRENT_DATE, 11)");
        Snapshot rows = crud.fetch("laeger", "1 = 1");
        Assertions.assertEquals(3, rows.getRecords().size());
        rows.export(List.of("lager"), System.out);
    }

}
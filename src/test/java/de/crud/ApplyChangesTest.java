package de.crud;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApplyChangesTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table laeger (lager varchar(3), bez varchar(30), time_aen date, nr_lfd integer, time_neu time default CURRENT_TIME,PRIMARY KEY (lager, nr_lfd))");
    }

    @AfterEach
    void tearDown() {
        crud.execute("drop table laeger");
        crud.close();
    }

    @Test
    void applyDelete() {
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('445', 'test989', CURRENT_DATE, 10)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test789', CURRENT_DATE, 11)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('234', 'test139', CURRENT_DATE, 13)");
        Snapshot reference = crud.fetch("laeger", null);
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('645', 'test489', CURRENT_DATE, 6)");
        Snapshot target = crud.fetch("laeger", null);
        ChangeSet change = reference.diff(target);
        Assertions.assertEquals(1, change.deleteRecs().size());
        Assertions.assertEquals(0, change.insertRecs().size());
        Assertions.assertEquals(0, change.updateRecs().size());
        crud.apply(change);
        Snapshot after = crud.fetch("laeger", null);
        ChangeSet likeBefore = reference.diff(after);
        Assertions.assertEquals(0, likeBefore.deleteRecs().size());
        Assertions.assertEquals(0, likeBefore.insertRecs().size());
        Assertions.assertEquals(0, likeBefore.updateRecs().size());
    }

    @Test
    void applyInsert() {
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('445', 'test989', CURRENT_DATE, 10)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('234', 'test139', CURRENT_DATE, 13)");
        Snapshot reference = crud.fetch("laeger", null);
        crud.execute("delete laeger where lager = '445'");
        Snapshot target = crud.fetch("laeger", null);
        ChangeSet change = reference.diff(target);
        Assertions.assertEquals(0, change.deleteRecs().size());
        Assertions.assertEquals(1, change.insertRecs().size());
        Assertions.assertEquals(0, change.updateRecs().size());
        crud.apply(change);
    }

    @Test
    void applyUpdate() {
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into laeger (lager, bez,time_aen, nr_lfd) values ('645', 'test489', CURRENT_DATE, 6)");
        Snapshot reference = crud.fetch("laeger", null);
        crud.execute("update laeger set bez = 'changed data' where lager = '645'");
        Snapshot target = crud.fetch("laeger", null);
        ChangeSet change = reference.diff(target);
        Assertions.assertEquals(0, change.deleteRecs().size());
        Assertions.assertEquals(0, change.insertRecs().size());
        Assertions.assertEquals(1, change.updateRecs().size());
        crud.apply(change);
    }

}

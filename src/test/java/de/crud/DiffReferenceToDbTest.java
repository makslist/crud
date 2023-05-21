package de.crud;

import org.junit.jupiter.api.*;

import java.util.*;

public class DiffReferenceToDbTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table tab (pk_char varchar(3), col_char varchar(30), col_date date, pk_int integer, col_time time default current_time, primary key (pk_char, pk_int))");

        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 1)");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 2)");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 3)");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test123', current_date, 1)");
    }

    @AfterEach
    void tearDown() {
        crud.execute("drop table tab");
        crud.close();
    }

    @Test
    void diffForOneRowDelete() {
        Snapshot reference = crud.fetch("tab", "pk_char = '222'");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");

        ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '222'"));
        Assertions.assertEquals(1, change.deleteRecs().size());
        Assertions.assertEquals(0, change.insertRecs().size());
        Assertions.assertEquals(0, change.updateRecs().size());
    }

    @Test
    void diffForOneRowUpdate() {
        Snapshot reference = crud.fetch("tab", "pk_char = '222'");
        crud.execute("update tab set col_char = 'changed data' where pk_char = '222' and pk_int = 1");

        ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '222'"));
        Assertions.assertEquals(1, change.updateRecs().size());
        Assertions.assertEquals(0, change.insertRecs().size());
        Assertions.assertEquals(0, change.deleteRecs().size());
    }

    @Test
    void diffForOneRowInsert() {
        Snapshot reference = crud.fetch("tab", "pk_char = '222'");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");

        ChangeSet change = crud.fetch("tab", "pk_char = '222'").delta(reference);
        Assertions.assertEquals(1, change.insertRecs().size());
        Assertions.assertEquals(0, change.updateRecs().size());
        Assertions.assertEquals(0, change.deleteRecs().size());
    }

    @Test
    void diffEachOneInsertUpdateDeleteRow() {
        Snapshot reference = crud.fetch("tab", "pk_char = '111'");
        crud.execute("delete tab where pk_char = '111' and pk_int = '1'");
        crud.execute("update tab set col_char = 'changed data' where pk_char = '111' and pk_int = 2");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test456', current_date, 4)");

        ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '111'"));
        Assertions.assertEquals(1, change.insertRecs().size());
        Assertions.assertEquals(1, change.updateRecs().size());
        Assertions.assertEquals(1, change.deleteRecs().size());
    }

    @Test
    void diffWithIgnoreColumn() {
        Snapshot reference = crud.fetch("tab", "pk_char = '111'");
        crud.execute("update tab set col_char = 'changed data' where pk_char = '111' and pk_int = 2");

        ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '111'"), Collections.singletonList("col_char"));
        Assertions.assertEquals(0, change.updateRecs().size());
        Assertions.assertEquals(0, change.insertRecs().size());
        Assertions.assertEquals(0, change.deleteRecs().size());
    }

}

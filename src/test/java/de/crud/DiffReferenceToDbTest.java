package de.crud;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

public class DiffReferenceToDbTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2(false);
        try {
            crud.execute("create table tab (pk_char varchar(3), col_char varchar(30), col_date date, pk_int integer, col_time time default current_time, primary key (pk_char, pk_int))");

            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 1)");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 2)");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 3)");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test123', current_date, 1)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        try {
            crud.execute("drop table tab");
            crud.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void diffForOneRowDelete() {
        try {
            Snapshot reference = crud.fetch("tab", "pk_char = '222'");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");

            ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '222'"), Collections.emptyList());
            Assertions.assertEquals(1, change.deleteRecs().size());
            Assertions.assertEquals(0, change.insertRecs().size());
            Assertions.assertEquals(0, change.updateRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void diffForOneRowUpdate() {
        try {
            Snapshot reference = crud.fetch("tab", "pk_char = '222'");
            crud.execute("update tab set col_char = 'changed data' where pk_char = '222' and pk_int = 1");

            ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '222'"), Collections.emptyList());
            Assertions.assertEquals(1, change.updateRecs().size());
            Assertions.assertEquals(0, change.insertRecs().size());
            Assertions.assertEquals(0, change.deleteRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void diffForOneRowInsert() {
        try {
            Snapshot reference = crud.fetch("tab", "pk_char = '222'");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");

            ChangeSet change = crud.fetch("tab", "pk_char = '222'").delta(reference, Collections.emptyList());
            Assertions.assertEquals(1, change.insertRecs().size());
            Assertions.assertEquals(0, change.updateRecs().size());
            Assertions.assertEquals(0, change.deleteRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void diffEachOneInsertUpdateDeleteRow() {
        try {
            Snapshot reference = crud.fetch("tab", "pk_char = '111'");
            crud.execute("delete tab where pk_char = '111' and pk_int = '1'");
            crud.execute("update tab set col_char = 'changed data' where pk_char = '111' and pk_int = 2");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test456', current_date, 4)");

            ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '111'"), Collections.emptyList());
            Assertions.assertEquals(1, change.insertRecs().size());
            Assertions.assertEquals(1, change.updateRecs().size());
            Assertions.assertEquals(1, change.deleteRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void diffWithIgnoreColumn() {
        try {
            Snapshot reference = crud.fetch("tab", "pk_char = '111'");
            crud.execute("update tab set col_char = 'changed data' where pk_char = '111' and pk_int = 2");

            ChangeSet change = reference.delta(crud.fetch("tab", "pk_char = '111'"), Collections.singletonList("col_char"));
            Assertions.assertEquals(0, change.updateRecs().size());
            Assertions.assertEquals(0, change.insertRecs().size());
            Assertions.assertEquals(0, change.deleteRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

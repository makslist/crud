package org.makslist.dbd;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

public class ApplyChangesTest {

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
    void applyDelete() {
        try {
            Snapshot reference = crud.fetch("tab");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test123', current_date, 2)");

            ChangeSet change = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(1, change.deleteRecs().size());
            Assertions.assertEquals(0, change.insertRecs().size());
            Assertions.assertEquals(0, change.updateRecs().size());
            crud.apply(change, false, false);

            ChangeSet empty = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(0, empty.deleteRecs().size());
            Assertions.assertEquals(0, empty.insertRecs().size());
            Assertions.assertEquals(0, empty.updateRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void applyInsert() {
        try {
            Snapshot reference = crud.fetch("tab");
            crud.execute("delete tab where pk_char = '222'");

            ChangeSet change = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(0, change.deleteRecs().size());
            Assertions.assertEquals(1, change.insertRecs().size());
            Assertions.assertEquals(0, change.updateRecs().size());
            crud.apply(change, false, false);

            ChangeSet empty = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(0, empty.deleteRecs().size());
            Assertions.assertEquals(0, empty.insertRecs().size());
            Assertions.assertEquals(0, empty.updateRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void applyUpdate() {
        try {
            Snapshot reference = crud.fetch("tab");
            crud.execute("update tab set col_char = 'changed data' where pk_char = '222'");

            ChangeSet change = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(0, change.deleteRecs().size());
            Assertions.assertEquals(0, change.insertRecs().size());
            Assertions.assertEquals(1, change.updateRecs().size());
            crud.apply(change, false, false);

            ChangeSet empty = reference.delta(crud.fetch("tab"), Collections.emptyList());
            Assertions.assertEquals(0, empty.deleteRecs().size());
            Assertions.assertEquals(0, empty.insertRecs().size());
            Assertions.assertEquals(0, empty.updateRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

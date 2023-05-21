package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;

public class ExecutingPlainSqlStatementTest {

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
    void executingDeleteAsSqlString() {
        Snapshot reference = crud.fetch("tab");
        crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");
        ChangeSet change = reference.delta(crud.fetch("tab"));
        Assertions.assertEquals(1, change.deleteRecs().size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());

        Assertions.assertTrue(reference.delta(crud.fetch("tab")).isEmpty());
    }

    @Test
    void executingUpdateAsSqlString() {
        Snapshot reference = crud.fetch("tab");
        crud.execute("update tab set col_char = 'changed data' where pk_char = '222' and pk_int = 1");
        ChangeSet change = reference.delta(crud.fetch("tab"));
        Assertions.assertEquals(1, change.updateRecs().size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());

        ChangeSet after = reference.delta(crud.fetch("tab"));
        Assertions.assertTrue(after.isEmpty());
    }

    @Test
    void executingInsertAsSqlString() {
        Snapshot reference = crud.fetch("tab");
        crud.execute("delete tab where pk_char = '222' and pk_int = 1");
        ChangeSet change = reference.delta(crud.fetch("tab"));
        Assertions.assertEquals(1, change.insertRecs().size());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());

        ChangeSet after = reference.delta(crud.fetch("tab"));
        Assertions.assertTrue(after.isEmpty());
    }

}

package de.crud;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

public class ExecutingPlainSqlStatementTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table tab (pk1 varchar(3), col1 varchar(30), col2 date, pk2 integer, time_neu time default CURRENT_TIME,PRIMARY KEY (pk1, pk2))");
    }

    @AfterEach
    void tearDown() {
        crud.execute("drop table tab");
        crud.close();
    }


    @Test
    void executingDeleteAsSqlString() {
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test123', CURRENT_DATE, 1)");
        Snapshot reference = crud.fetch("tab", null);
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test456', CURRENT_DATE, 2)");
        ChangeSet change = reference.diff(crud.fetch("tab", null));
        Assertions.assertFalse(change.isEmpty());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());
        ChangeSet after = reference.diff(crud.fetch("tab", null));
        Assertions.assertTrue(after.isEmpty());
    }


    @Test
    void executingUpdateAsSqlString() {
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test456', CURRENT_DATE, 2)");
        Snapshot reference = crud.fetch("tab", null);
        crud.execute("update tab set col1 = 'changed data' where pk1 = '123' and pk2 = 2");
        ChangeSet change = reference.diff(crud.fetch("tab", null));
        Assertions.assertFalse(change.isEmpty());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());
        ChangeSet after = reference.diff(crud.fetch("tab", null));
        Assertions.assertTrue(after.isEmpty());
    }

    @Test
    void executingInsertAsSqlString() {
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test456', CURRENT_DATE, 2)");
        Snapshot reference = crud.fetchTable("tab");
        crud.execute("delete tab where pk1 = '123' and pk2 = 2");
        Snapshot target = crud.fetchTable("tab");
        ChangeSet change = reference.diff(target);
        Assertions.assertFalse(change.isEmpty());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        crud.write(change, baos);
        crud.execute(baos.toString());
        ChangeSet after = reference.diff(crud.fetchTable("tab"));
        Assertions.assertTrue(after.isEmpty());
    }

}

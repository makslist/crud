package de.crud;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsoleDisplayDiffTest {

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
        crud.execute("insert into tab (pk1, col1, col2, pk2) values('123', 'test123', CURRENT_DATE, 1)");
        crud.execute("insert into tab (pk1, col1, col2, pk2) values('123', 'test456', CURRENT_DATE, 2)");
        Snapshot reference = crud.fetchTable("tab");
        crud.execute("delete tab where pk1 = '123' and pk2 = '1'");
        crud.execute("update tab set col1 = 'changed data' where pk1 = '123' and pk2 = 2");
        crud.execute("insert into tab (pk1, col1, col2, pk2) values ('123', 'test456', CURRENT_DATE, 3)");
        Snapshot after = crud.fetchTable("tab");
        ChangeSet change = reference.diff(after);
        change.displayDiff();
    }

}

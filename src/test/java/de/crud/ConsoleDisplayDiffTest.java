package de.crud;

import org.junit.jupiter.api.*;

import java.sql.*;

public class ConsoleDisplayDiffTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
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
    void executingDeleteAsSqlString() {
        try {
            Snapshot reference = crud.fetch("tab");
            crud.execute("delete tab where pk_char = '111' and pk_int = '1'");
            crud.execute("update tab set col_char = 'changed data' where pk_char = '111' and pk_int = 2");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('222', 'test456', current_date, 2)");
            Snapshot after = crud.fetch("tab");
            ChangeSet change = reference.delta(after);
            change.displayDiff();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

package de.crud;

import org.junit.jupiter.api.*;

import java.sql.*;

public class InsertWithCreateTableTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2(false);
        try {
            crud.execute("create table tab (pk_char varchar(3), col_char varchar(30), col_date date, pk_int integer, col_time time default current_time, primary key (pk_char, pk_int))");
            crud.execute("insert into tab (pk_char, col_char, col_date, pk_int) values ('111', 'test123', current_date, 1)");
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
    void applyInsertForNonExistingTable() {
        try {
            Snapshot reference = crud.fetch("tab");
            crud.execute("drop table tab");

            crud.existsOrCreate(reference);
            ChangeSet change = reference.delta(crud.fetch("tab"));
            Assertions.assertEquals(1, change.insertRecs().size());
            crud.apply(change, false,false);

            ChangeSet empty = reference.delta(crud.fetch("tab"));
            Assertions.assertEquals(0, empty.deleteRecs().size());
            Assertions.assertEquals(0, empty.insertRecs().size());
            Assertions.assertEquals(0, empty.updateRecs().size());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}

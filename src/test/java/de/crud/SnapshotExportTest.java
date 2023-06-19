package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class SnapshotExportTest {

    private static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {
        @Override
        public void write(int i) {
        }
    };
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
    void descPkOfTable() {
        try {
            List<String> pk = crud.descPkOf("tab");
            Assertions.assertEquals("pk_char", pk.get(0));
            Assertions.assertEquals("pk_int", pk.get(1));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void fetchSnapshotFull() {
        try {
            Snapshot rows = crud.fetch("tab", "pk_char = '111'");
            Assertions.assertEquals(3, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportSnapshotPartition() {
        try {
            Snapshot rows = crud.fetch("tab");
            Assertions.assertEquals(4, rows.getRecords().size());
            rows.export(Collections.singletonList("pk_char"), new File("./"));
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
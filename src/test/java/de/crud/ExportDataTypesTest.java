package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.*;

public class ExportDataTypesTest {

    private static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {
        @Override
        public void write(int i) throws IOException {
        }
    };

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2(false);
        try {
            crud.execute("create table numtypes (pk varchar(3), col2 smallint, col3 tinyint, col4 integer, col5 bigint, col6 numeric, col7 decimal, col8 float, col9 real, col10 double, primary key (pk))");
            crud.execute("create table datetypes (pk varchar(3), col12 date, col13 time, col14 timestamp, primary key (pk))");
            crud.execute("create table binarytypes (pk varchar(3), col11 boolean, col17 binary(8), col18 bit, col20 null, primary key (pk))");
            crud.execute("create table lobtypes (pk varchar(3), col19 nclob, col22 clob, col23 blob, primary key (pk))");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        try {
            crud.execute("drop table numtypes");
            crud.execute("drop table datetypes");
            crud.execute("drop table binarytypes");
            crud.execute("drop table lobtypes");
            crud.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportNumbers() {
        try {
            crud.execute("insert into numtypes (pk, col2, col3, col4, col5, col6, col7, col8, col9, col10) values ('abc', 1, 2, 3, 1, 5, 6, 7, 8, 9)");
            Snapshot rows = crud.fetch("numtypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportDate() {
        try {
            crud.execute("insert into datetypes (pk, col12, col13, col14) values ('abc', CURRENT_DATE, CURRENT_TIME, CURRENT_DATE)");
            Snapshot rows = crud.fetch("datetypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportBinary() {
        try {
            crud.execute("insert into binarytypes (pk, col11, col17, col18, col20) values ('abc', 1, CAST(X'00000001' AS BINARY(4)), 3, 4)");
            Snapshot rows = crud.fetch("binarytypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportLob() {
        try {
            crud.execute("insert into lobtypes (pk, col19, col22, col23) values ('abc', 1, 2, 3)");
            Snapshot rows = crud.fetch("lobtypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}

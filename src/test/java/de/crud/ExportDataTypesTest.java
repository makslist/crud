package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.*;

public class ExportDataTypesTest {

    private static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {
        @Override
        public void write(int i) {
        }
    };

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2(false);
    }

    @AfterEach
    void tearDown() {
        try {
            crud.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportNumbers() {
        try {
            crud.execute("create table numtypes (pk varchar(3), col1 smallint, col2 tinyint, col3 integer, col4 bigint, col5 numeric, col6 decimal, col7 float, col8 real, col9 double, primary key (pk))");
            crud.execute("insert into numtypes (pk, col1, col2, col3, col4, col5, col6, col7, col8, col9) values ('abc', 1, 2, 3, 1, 5, 6, 7, 8, 9)");
            Snapshot rows = crud.fetch("numtypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
            crud.execute("drop table numtypes");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportDate() {
        try {
            crud.execute("create table datetypes (pk varchar(3), col11 date, col12 time, col13 timestamp, primary key (pk))");
            crud.execute("insert into datetypes (pk, col11, col12, col13) values ('abc', CURRENT_DATE, CURRENT_TIME, CURRENT_DATE)");
            Snapshot rows = crud.fetch("datetypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
            crud.execute("drop table datetypes");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportBinary() {
        try {
            crud.execute("create table binarytypes (pk varchar(3), col21 boolean, col22 binary(8), col23 bit, col24 null, primary key (pk))");
            crud.execute("insert into binarytypes (pk, col21, col22, col23, col24) values ('abc', 1, X'00000001', 3, null)");
            Snapshot rows = crud.fetch("binarytypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
            crud.execute("drop table binarytypes");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportLob() {
        try {
            crud.execute("create table lobtypes (pk varchar(3), col31 nclob, col32 clob, col33 blob, primary key (pk))");
            crud.execute("insert into lobtypes (pk, col31, col32, col33) values ('abc', 1, 2, 3)");
            Snapshot rows = crud.fetch("lobtypes", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
            crud.execute("drop table lobtypes");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void exportRaw() {
        try {
            crud.execute("create table raws (pk varchar(3), col41 raw, primary key (pk))");
            crud.execute("insert into raws (pk, col41) values ('abc', X'00000001')");
            Snapshot rows = crud.fetch("raws", "pk = 'abc'");
            Assertions.assertEquals(1, rows.getRecords().size());
            rows.export(NULL_OUTPUT_STREAM);
            crud.execute("drop table raws");
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}

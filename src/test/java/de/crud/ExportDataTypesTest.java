package de.crud;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;

public class ExportDataTypesTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table numtypes (pk varchar(3), col2 SMALLINT, col3 TINYINT, col4 INTEGER, col5 BIGINT, col6 NUMERIC, col7 DECIMAL, col8 FLOAT, col9 REAL, col10 DOUBLE, PRIMARY KEY (pk))");
        crud.execute("create table datetypes (pk varchar(3), col12 DATE, col13 TIME, col14 TIMESTAMP, PRIMARY KEY (pk))");
        crud.execute("create table binarytypes (pk varchar(3), col11 BOOLEAN, col17 BINARY(8), col18 BIT, col20 NULL, PRIMARY KEY (pk))");
        crud.execute("create table lobtypes (pk varchar(3), col19 NCLOB, col22 CLOB, col23 BLOB, PRIMARY KEY (pk))");
    }

    @AfterEach
    void tearDown() {
        crud.execute("drop table numtypes");
        crud.execute("drop table datetypes");
        crud.execute("drop table binarytypes");
        crud.execute("drop table lobtypes");
        crud.close();
    }

    @Test
    void exportNumbers() {
        crud.execute("insert into numtypes (pk, col2, col3, col4, col5, col6, col7, col8, col9, col10) values ('abc', 1, 2, 3, 1, 5, 6, 7, 8, 9)");
        Snapshot rows = crud.fetch("numtypes", "pk = 'abc'");
        Assertions.assertEquals(1, rows.getRecords().size());
        rows.export(OutputStream.nullOutputStream());
    }

    @Test
    void exportDate() {
        crud.execute("insert into datetypes (pk, col12, col13, col14) values ('abc', CURRENT_DATE, CURRENT_TIME, CURRENT_DATE)");
        Snapshot rows = crud.fetch("datetypes", "pk = 'abc'");
        Assertions.assertEquals(1, rows.getRecords().size());
        rows.export(OutputStream.nullOutputStream());
    }

    @Test
    void exportBinary() {
        crud.execute("insert into binarytypes (pk, col11, col17, col18, col20) values ('abc', 1, CAST(X'00000001' AS BINARY(4)), 3, 4)");
        Snapshot rows = crud.fetch("binarytypes", "pk = 'abc'");
        Assertions.assertEquals(1, rows.getRecords().size());
        rows.export(OutputStream.nullOutputStream());
    }

    @Test
    void exportLob() {
        crud.execute("insert into lobtypes (pk, col19, col22, col23) values ('abc', 1, 2, 3)");
        Snapshot rows = crud.fetch("lobtypes", "pk = 'abc'");
        Assertions.assertEquals(1, rows.getRecords().size());
        rows.export(OutputStream.nullOutputStream());
    }

}

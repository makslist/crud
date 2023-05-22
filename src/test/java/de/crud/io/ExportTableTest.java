package de.crud.io;

import de.crud.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;

public class ExportTableTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
        crud.execute("create table numtypes (pk varchar(3), col2 smallint, col3 tinyint, col4 integer, col5 bigint, col6 numeric, col7 decimal, col8 float, col9 real, col10 double, primary key (pk))");
        crud.execute("create table datetypes (pk varchar(3), col12 date, col13 time, col14 timestamp, primary key (pk))");
        crud.execute("create table binarytypes (pk varchar(3), col11 boolean, col17 binary(8), col18 bit, col20 null, primary key (pk))");
        crud.execute("create table lobtypes (pk varchar(3), col19 nclob, col22 clob, col23 blob, primary key (pk))");
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
    void exportNumbers() throws IOException {
        crud.execute("insert into numtypes (pk, col2, col3, col4, col5, col6, col7, col8, col9, col10) values ('abc', 1, 2, 3, 1, 5, 6, 7, 8, 9)");
        Snapshot snapshot = crud.fetch("numtypes", "pk = 'abc'");
        String filename = "./" + "test" + ".snapshot";
        snapshot.export(Files.newOutputStream(Paths.get(filename)));
    }

}
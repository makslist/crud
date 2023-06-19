package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

public class MassImportTest {

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
    void applyInsertForNonExistingTable() {
        String table = "best_history_korr";
        try {
            File file = new File("/home/maks/crud/" + table + ".snapshot");
            Snapshot reference = Snapshot.read(file);

            crud.existsOrCreate(reference, true);
            ChangeSet change = crud.delta(reference, Collections.emptyList());
            crud.apply(change, false, false);

            Snapshot s1 = crud.fetch(table);
            s1.export(Files.newOutputStream(file.toPath()));
            ChangeSet empty = reference.delta(s1, Collections.emptyList());
            Assertions.assertEquals(0, empty.deleteRecs().size());
            Assertions.assertEquals(0, empty.insertRecs().size());
            Assertions.assertEquals(0, empty.updateRecs().size());
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}

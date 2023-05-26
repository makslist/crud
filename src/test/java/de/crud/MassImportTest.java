package de.crud;

import org.junit.jupiter.api.*;

import java.io.*;
import java.util.*;

public class MassImportTest {

    Crud crud;

    @BeforeEach
    void setUp() {
        crud = Crud.connectH2();
    }

    @AfterEach
    void tearDown() {
        crud.close();
    }

    @Test
    void applyInsertForNonExistingTable() {
        Snapshot reference = Snapshot.read(new File("/home/maks/crud/sst_out_data.snapshot"));

        crud.existsOrCreate(reference);
        ChangeSet change = crud.delta(reference, Collections.emptyList());
        crud.apply(change, false);

        ChangeSet empty = crud.delta(reference, Collections.emptyList());
        Assertions.assertEquals(0, empty.deleteRecs().size());
        Assertions.assertEquals(0, empty.insertRecs().size());
        Assertions.assertEquals(0, empty.updateRecs().size());
    }

}

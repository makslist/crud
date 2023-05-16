package de.crud.io;

import de.crud.Snapshot;
import org.junit.jupiter.api.Test;

import java.io.File;

public class ReadFileTest {

    @Test
    void readOneFile() {
        Snapshot entry = Snapshot.read(new File("/home/maks/ImportTest.entr"));
    }

}

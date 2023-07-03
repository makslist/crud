package de.crud.io;

import de.crud.*;
import org.junit.jupiter.api.*;

import java.io.*;

public class ReadFileTest {

    @Test
    void readFile() {
        try {
            Snapshot.read(new File("./tab.snapshot"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

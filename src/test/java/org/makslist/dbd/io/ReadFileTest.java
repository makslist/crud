package org.makslist.dbd.io;

import org.junit.jupiter.api.*;
import org.makslist.dbd.*;

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

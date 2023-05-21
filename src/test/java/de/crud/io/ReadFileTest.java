package de.crud.io;

import de.crud.*;
import org.junit.jupiter.api.*;

import java.io.*;

public class ReadFileTest {

    @Test
    void readFile() {
        Snapshot.read(new File("./test.snapshot"));
    }

}

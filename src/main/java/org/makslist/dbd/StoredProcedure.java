package org.makslist.dbd;

import java.sql.*;
import java.util.*;

public class StoredProcedure {

    public static final String GET_ALL = "SELECT OBJECT_NAME FROM ALL_PROCEDURES WHERE OWNER = UPPER(?) AND OBJECT_NAME LIKE UPPER(?) AND PROCEDURE_NAME IS NULL";
    public static final String GET_DDL = "SELECT OBJECT_NAME, OBJECT_TYPE, DBMS_METADATA.GET_DDL(OBJECT_TYPE, OBJECT_NAME, OWNER) FROM (SELECT * FROM ALL_PROCEDURES WHERE OWNER = UPPER(?) AND OBJECT_NAME = UPPER(?) AND PROCEDURE_NAME IS NULL)";

    public static List<String> all(String owner, String pattern, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(GET_ALL)) {
            stmt.setString(1, owner);
            stmt.setString(2, pattern);
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            List<String> procedureNames = new ArrayList<>();
            while (rs.next())
                procedureNames.add(rs.getString(1));
            return procedureNames;
        }
    }

    public static StoredProcedure ddl(String owner, String name, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(GET_DDL)) {
            stmt.setString(1, owner);
            stmt.setString(2, name);
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            if (rs.next())
                return new StoredProcedure(rs.getString(1), rs.getString(2), rs.getString(3));
            throw new SQLException();
        }
    }

    private String name;
    private String type;
    private String source = null;

    public StoredProcedure() {
    }

    public StoredProcedure(String name, String type, String source) {
        this.name = name;
        this.type = type;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getSource() {
        return source;
    }

    public String toString() {
        return source;
    }

}

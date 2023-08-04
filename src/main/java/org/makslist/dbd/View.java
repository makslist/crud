package org.makslist.dbd;

import java.sql.*;
import java.util.*;

public class View {

    public static String GET_ALL = "SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = UPPER(?) AND VIEW_NAME LIKE UPPER(?)";
    public static String GET_DDL = "SELECT DBMS_METADATA.GET_DDL('VIEW', UPPER(?)) FROM DUAL";

    public static List<String> all(String owner, String pattern, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(GET_ALL)) {
            stmt.setString(1, owner);
            stmt.setString(2, pattern);
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            List<String> viewNames = new ArrayList<>();
            while (rs.next())
                viewNames.add(rs.getString(1));
            return viewNames;
        }
    }

    public static View ddl(String name, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(GET_DDL)) {
            stmt.setString(1, name);
            stmt.setFetchSize(1000);
            ResultSet rs = stmt.executeQuery();
            if (rs.next())
                return new View(rs.getString(1));
            throw new SQLException();
        }
    }

    private String source = null;

    public View() {
    }

    public View(String source) {
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public String toString() {
        return source;
    }

}

package com.webank.wedatasphere.linkis.metadatamanager.service;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    private SqlConnection connection;

    @Before
    public void before() {
        try {
            connection = new SqlConnection("192.168.0.110", 5236, "TESTUSER", "test!0819", new HashMap<>());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void test() throws SQLException {
        List<String> allDatabases = connection.getAllDatabases();
        System.out.println(allDatabases);
    }

}

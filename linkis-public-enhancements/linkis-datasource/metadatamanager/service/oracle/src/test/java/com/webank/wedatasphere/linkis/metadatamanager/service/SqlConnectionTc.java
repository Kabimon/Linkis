package com.webank.wedatasphere.linkis.metadatamanager.service;

import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    @Test
    public void test01() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 1521, "qbitdata", "test!071471", "ORCL", new HashMap<>());
        List<String> allDatabases = connection.getAllDatabases();
        System.out.println(allDatabases);
    }

}

package com.webank.wedatasphere.linkis.metadatamanager.service;

import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    @Test
    public void test01() {
        try {
            SqlConnection db2inst1 = new SqlConnection("192.168.0.60", 50000, "db2inst1", "test!0809", "SAMPLE", new HashMap<>());
            List<String> allDatabases = db2inst1.getAllDatabases();
            System.out.println(allDatabases);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}

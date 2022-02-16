package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTestCase {

    @Test
    public void test01() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 54321, "TESTUSER", "test!0819", "my_db", new HashMap<>());
        List<String> allDatabases = connection.getAllDatabases();
        System.out.println(allDatabases);
    }

    @Test
    public void test02() throws SQLException, ClassNotFoundException {
//        SqlConnection connection = new SqlConnection("192.168.0.133", 54321, "system", "123456", "test", new HashMap<>());
        SqlConnection connection = new SqlConnection("192.168.0.71", 54321, "TESTUSER", "test!0819", "my_db", new HashMap<>());
        List<String> allTables = connection.getAllTables("public");
        System.out.println(allTables);
    }

    @Test
    public void test03() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 54321, "TESTUSER", "test!0819", "my_db", new HashMap<>());
        List<MetaColumnInfo> allColumns = connection.getColumns("public", "pathman_config_params");
        for (MetaColumnInfo column : allColumns) {
            System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
        }
    }

}

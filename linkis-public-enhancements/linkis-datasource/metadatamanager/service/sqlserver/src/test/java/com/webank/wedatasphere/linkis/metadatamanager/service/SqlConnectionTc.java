package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    @Test
    public void test01() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 1433, "sa", "123456asd@", new HashMap<>());
        List<String> allDatabases = connection.getAllDatabases();
        System.out.println(allDatabases);
    }

    @Test
    public void test02() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 1433, "sa", "123456asd@", new HashMap<>());
        List<String> allTables = connection.getAllTables("ReportServer");
        System.out.println(allTables);
    }

    @Test
    public void test03() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.71", 1433, "sa", "123456asd@", new HashMap<>());
        List<MetaColumnInfo> allColumns = connection.getColumns("ReportServer", "Users");
        for (MetaColumnInfo column : allColumns) {
            System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
        }
    }
}

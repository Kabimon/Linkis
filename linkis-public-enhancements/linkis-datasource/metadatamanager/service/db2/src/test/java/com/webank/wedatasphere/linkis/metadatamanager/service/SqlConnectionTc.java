package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    @Test
    public void testCmd() {
//        String command = "db2cmd /c "
    }

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

    @Test
    public void testGetTables() {
        try {
            SqlConnection db2inst1 = new SqlConnection("192.168.0.60", 50000, "db2inst1", "test!0809", "SAMPLE", new HashMap<>());
            List<String> allTables = db2inst1.getAllTables("DB2INST1");
            System.out.println(allTables);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testGetColumns() {
        try {
            SqlConnection db2inst1 = new SqlConnection("192.168.0.60", 50000, "db2inst1", "test!0809", "SAMPLE", new HashMap<>());
            List<MetaColumnInfo> columns = db2inst1.getColumns("DB2INST1", "EMPLOYEE");
            for (MetaColumnInfo column : columns) {
                System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}

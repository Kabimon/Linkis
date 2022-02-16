package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class SqlConnectionTc {

    @Test
    public void test02() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.17", 5236, "SYSDBA", "SYSDBA", "DMSERVER", new HashMap<>());
        List<String> allTables = connection.getAllTables("GJTEST");
        System.out.println(allTables);
    }

    @Test
    public void test03() throws SQLException, ClassNotFoundException {
        SqlConnection connection = new SqlConnection("192.168.0.17", 5236, "SYSDBA", "SYSDBA", "DMSERVER", new HashMap<>());
        List<MetaColumnInfo> allColumns = connection.getColumns("GJTEST", "TBL_USER");
        for (MetaColumnInfo column : allColumns) {
            System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
        }
    }

}

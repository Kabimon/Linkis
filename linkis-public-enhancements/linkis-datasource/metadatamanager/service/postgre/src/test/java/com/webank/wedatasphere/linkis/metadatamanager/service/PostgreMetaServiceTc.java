package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import com.webank.wedatasphere.linkis.metadatamanager.common.service.MetadataService;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgreMetaServiceTc {

    private Map<String, Object> noDbParams;
    private Map<String, Object> params;

    @Before
    public void before() {
        params = new HashMap<>();
        params.put("host", "localhost");
        params.put("port", 15432);
        params.put("username", "postgres");
        params.put("password", "123456");
        params.put("database", "gujian");

        noDbParams = new HashMap<>();
        noDbParams.put("host", "localhost");
        noDbParams.put("port", 15432);
        noDbParams.put("username", "postgres");
        noDbParams.put("password", "123456");
    }

    @Test
    public void testGetDatabases() {
        MetadataService metadataService = new PostgreMetaService();
        List<String> databases = metadataService.getDatabases("", noDbParams);
        System.out.println(databases);
    }

    @Test
    public void testGetTables() {
        MetadataService metadataService = new PostgreMetaService();
        // 这里的 public 参数是 pg 中的 schema
        List<String> tables = metadataService.getTables("", params, "public");
        System.out.println(tables);
    }

    @Test
    public void testGetColumns() {
        MetadataService metadataService = new PostgreMetaService();
        List<MetaColumnInfo> columns = metadataService.getColumns("", params, "public", "gujian_table1");

        for (MetaColumnInfo column : columns) {
            System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
        }
    }

}

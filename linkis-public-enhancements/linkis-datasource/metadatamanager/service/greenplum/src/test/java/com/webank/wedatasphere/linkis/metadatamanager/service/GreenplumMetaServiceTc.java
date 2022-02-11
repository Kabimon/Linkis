package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import com.webank.wedatasphere.linkis.metadatamanager.common.service.MetadataService;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GreenplumMetaServiceTc {
    private Map<String, Object> noDbParams;
    private Map<String, Object> params;

    @Before
    public void before() {
        params = new HashMap<>();
        params.put("host", "192.168.0.30");
        params.put("port", 5432);
        params.put("username", "gpadmin");
        params.put("password", "123456");
        params.put("database", "gujian_test");

        noDbParams = new HashMap<>();
        noDbParams.put("host", "192.168.0.30");
        noDbParams.put("port", 5432);
        noDbParams.put("username", "gpadmin");
        noDbParams.put("password", "123456");
        noDbParams.put("database", "postgres");
    }

    @Test
    public void testGetDatabases() {
        MetadataService metadataService = new GreenplumMetaService();
        List<String> databases = metadataService.getDatabases("", noDbParams);
        System.out.println(databases);
    }

    @Test
    public void testGetTables() {
        MetadataService metadataService = new GreenplumMetaService();
        // 这里的 public 参数是 pg 中的 schema
        List<String> tables = metadataService.getTables("", params, "public");
        System.out.println(tables);
    }

    @Test
    public void testGetColumns() {
        MetadataService metadataService = new GreenplumMetaService();
        List<MetaColumnInfo> columns = metadataService.getColumns("", params, "public", "t_user");

        for (MetaColumnInfo column : columns) {
            System.out.println(column.getIndex() + "\t" + column.getName() + "\t" + column.getType());
        }
    }
}

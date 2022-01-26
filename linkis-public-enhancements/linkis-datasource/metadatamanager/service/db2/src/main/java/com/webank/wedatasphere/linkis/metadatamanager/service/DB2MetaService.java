package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.service.AbstractMetaService;
import com.webank.wedatasphere.linkis.metadatamanager.common.service.MetadataConnection;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DB2MetaService extends AbstractMetaService<SqlConnection> {
    @Override
    public MetadataConnection<SqlConnection> getConnection(String operator, Map<String, Object> params) throws Exception {
        return null;
    }
}

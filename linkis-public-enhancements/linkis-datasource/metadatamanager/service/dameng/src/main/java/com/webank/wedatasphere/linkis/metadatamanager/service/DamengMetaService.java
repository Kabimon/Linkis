package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.metadatamanager.common.service.AbstractMetaService;
import com.webank.wedatasphere.linkis.metadatamanager.common.service.MetadataConnection;

import java.util.Map;

public class DamengMetaService extends AbstractMetaService<SqlConnection> {
    @Override
    public MetadataConnection<SqlConnection> getConnection(String operator, Map<String, Object> params) throws Exception {
        return null;
    }
}

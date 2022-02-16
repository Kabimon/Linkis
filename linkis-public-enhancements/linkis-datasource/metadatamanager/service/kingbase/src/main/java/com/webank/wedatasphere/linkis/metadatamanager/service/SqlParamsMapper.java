package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.common.conf.CommonVars;

public class SqlParamsMapper {
    public static final CommonVars<String> PARAM_SQL_HOST =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.host", "host");

    public static final CommonVars<String> PARAM_SQL_PORT =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.port", "port");

    public static final CommonVars<String> PARAM_SQL_USERNAME =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.username", "username");

    public static final CommonVars<String> PARAM_SQL_PASSWORD =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.password", "password");

    public static final CommonVars<String> PARAM_SQL_DATABASE =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.database", "database");

    public static final CommonVars<String> PARAM_SQL_EXTRA_PARAMS =
            CommonVars.apply("wds.linkis.server.mdm.service.sql.params", "params");
}

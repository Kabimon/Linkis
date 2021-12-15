package com.webank.wedatasphere.linkis.computation.client;


import com.google.gson.Gson;
import com.webank.wedatasphere.linkis.common.conf.Configuration;
import com.webank.wedatasphere.linkis.computation.client.once.simple.SubmittableSimpleOnceJob;
import com.webank.wedatasphere.linkis.computation.client.utils.LabelKeyUtils;

import java.util.HashMap;
import java.util.Map;

public class DataXOnceJobTest2 {
    public static void main(String[] args) {
        LinkisJobClient.config().setDefaultServerUrl("http://124.70.31.149:8088");
        /**
         */
        String code = "{\n" +
                "    \"job\": {\n" +
                "        \"content\":[\n" +
                "            {\n" +
                "                \"reader\": {\n" +
                "                    \"name\": \"mysqlreader\", \n" +
                "                    \"parameter\": {\n" +
                "                        \"username\":\"scm\",\n"+
                "                        \"password\": \"scm_@casc2f\", \n" +
                "                        \"connection\": [\n" +
                "                            {\n" +
                "                                \"jdbcUrl\":\"jdbc:mysql://192.168.0.66:3306/authorization_test\", \n" +
                "                                \"table\": [\"user\"]\n" +
                "                            }\n" +
                "                         ]\n" +
                "                       }\n" +
                "                   }\n" +
                "                }, \n" +
                "                \"writer\": {\n" +
                "                    \"name\": \"mysqlwriter\", \n" +
                "                    \"parameter\": {\n" +
                "                        \"username\": \"scm\",\n" +
                "                        \"password\": \"scm_@casc2f\", \n" +
                "                        \"preSql\": [], \n" +
                "                        \"connection\": [\n" +
                "                            {\n" +
                "                                \"jdbcUrl\":\"jdbc:mysql://192.168.0.66:3306/authorization_test\", \n" +
                "                                \"table\": [\"user\"]\n" +
                "                            }\n" +
                "                        ]\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        ], \n" +
                "        \"setting\": {\n" +
                "            \"speed\": {\n" +
                "                \"channel\": \"4\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        Map<String,String> rwMaps = new HashMap<>();
        rwMaps.put("reader","mysqlreader");
        rwMaps.put("writer","mysqlwriter");
        SubmittableSimpleOnceJob onceJob = LinkisJobClient.once().simple().builder().setDescription(new Gson().toJson(rwMaps)).setCreateService("DataX-Test")
                .setMaxSubmitTime(300000)
                .addLabel(LabelKeyUtils.ENGINE_TYPE_LABEL_KEY(), "datax-3.0.0")
                .addLabel(LabelKeyUtils.USER_CREATOR_LABEL_KEY(), "hdfs-datax")
                .addLabel(LabelKeyUtils.ENGINE_CONN_MODE_LABEL_KEY(), "once")
                .addStartupParam(Configuration.IS_TEST_MODE().key(), true)
                .addExecuteUser("hdfs").addJobContent("runType", "appconn").addJobContent("code", code).addSource("jobName", "OnceJobTest")
                .build();
        onceJob.submit();
        System.out.println(onceJob.getId());

        onceJob.waitForCompleted();
        System.out.println(onceJob.getStatus());
        LinkisJobMetrics jobMetrics = onceJob.getJobMetrics();
        System.out.println(jobMetrics.getMetrics());
    }
}

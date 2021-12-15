package com.webank.wedatasphere.linkis.computation.client;


import com.google.gson.Gson;
import com.webank.wedatasphere.linkis.common.conf.Configuration;
import com.webank.wedatasphere.linkis.computation.client.once.simple.SubmittableSimpleOnceJob;
import com.webank.wedatasphere.linkis.computation.client.utils.LabelKeyUtils;

import java.util.HashMap;
import java.util.Map;

public class DataXOnceJobTest {
    public static void main(String[] args) {
        LinkisJobClient.config().setDefaultServerUrl("http://121.36.12.247:8088");
        String code = "{\n" +
                "    \"job\": {\n" +
                "        \"content\":[\n" +
                "            {\n" +
                "                \"reader\": {\n" +
                "                    \"name\": \"txtfilereader\", \n" +
                "                    \"parameter\": {\n" +
                "                        \"path\":[\"/opt/install/datax/data/test1.csv\"],\n" +
                "                        \"encoding\":\"gbk\",\n" +
                "                        \"column\": [\n" +
                "                            {\n" +
                "                                \"index\":0,\n" +
                "                                \"type\":\"string\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"index\":1,\n" +
                "                                \"type\":\"string\"\n" +
                "                            }\n" +
                "                        ], \n" +
                "                        \"fileldDelimiter\":\",\"\n" +
                "                    }\n" +
                "                }, \n" +
                "                \"writer\": {\n" +
                "                    \"name\": \"mysqlwriter\", \n" +
                "                    \"parameter\": {\n" +
                "                        \"username\": \"root\",\n" +
                "                        \"password\": \"MTIzNDU2\", \n" +
                "                        \"column\": [\n" +
                "                            \"i\",\n" +
                "                            \"j\"\n" +
                "                        ],\n" +
                "                        \"preSql\": [], \n" +
                "                        \"connection\": [\n" +
                "                            {\n" +
                "                                \"jdbcUrl\":\"jdbc:mysql://172.24.2.61:3306/test\", \n" +
                "                                \"table\": [\"testtab\"]\n" +
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
        rwMaps.put("reader","textfilereader");
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

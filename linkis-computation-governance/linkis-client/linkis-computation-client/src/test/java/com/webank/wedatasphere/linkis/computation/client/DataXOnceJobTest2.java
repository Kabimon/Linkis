package com.webank.wedatasphere.linkis.computation.client;


import com.google.gson.Gson;
import com.webank.wedatasphere.linkis.common.conf.Configuration;
import com.webank.wedatasphere.linkis.computation.client.once.simple.SubmittableSimpleOnceJob;
import com.webank.wedatasphere.linkis.computation.client.utils.LabelKeyUtils;

import java.util.HashMap;
import java.util.Map;

public class DataXOnceJobTest2 {
    public static void main(String[] args) {
        LinkisJobClient.config().setDefaultServerUrl("http://124.70.31.149:20088");
        /**
         */
        String code = "{\n" +
                "\t\"content\":[\n" +
                "\t\t{\n" +
                "\t\t\t\"reader\":{\n" +
                "\t\t\t\t\"parameter\":{\n" +
                "\t\t\t\t\t\"password\":\"scm_@casc2f\",\n" +
                "\t\t\t\t\t\"column\":[\n" +
                "\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\"name\":\"id\",\n" +
                "\t\t\t\t\t\t\t\"type\":\"INT\"\n" +
                "\t\t\t\t\t\t},\n" +
                "\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\"name\":\"age\",\n" +
                "\t\t\t\t\t\t\t\"type\":\"INT\"\n" +
                "\t\t\t\t\t\t}\n" +
                "\t\t\t\t\t],\n" +
                "\t\t\t\t\t\"connection\":[\n" +
                "\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\"jdbcUrl\":[\n" +
                "\t\t\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\t\t\"database\":\"test\",\n" +
                "\t\t\t\t\t\t\t\t\t\"port\":\"3306\",\n" +
                "\t\t\t\t\t\t\t\t\t\"host\":\"192.168.0.66\"\n" +
                "\t\t\t\t\t\t\t\t}\n" +
                "\t\t\t\t\t\t\t],\n" +
                "\t\t\t\t\t\t\t\"table\":[\n" +
                "\t\t\t\t\t\t\t\t\"t_psn\"\n" +
                "\t\t\t\t\t\t\t]\n" +
                "\t\t\t\t\t\t}\n" +
                "\t\t\t\t\t],\n" +
                "\t\t\t\t\t\"username\":\"scm\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"name\":\"mysqlreader\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"writer\":{\n" +
                "\t\t\t\t\"parameter\":{\n" +
                "\t\t\t\t\t\"path\":\"/user/hive/warehouse/psn\",\n" +
                "\t\t\t\t\t\"fileName\":\"psn\",\n" +
                "\t\t\t\t\t\"hiveMetastoreUris\":\"thrift://192.168.0.111:9083\",\n" +
                "\t\t\t\t\t\"column\":[\n" +
                "\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\"index\":0,\n" +
                "\t\t\t\t\t\t\t\"type\":\"int\"\n" +
                "\t\t\t\t\t\t},\n" +
                "\t\t\t\t\t\t{\n" +
                "\t\t\t\t\t\t\t\"index\":1,\n" +
                "\t\t\t\t\t\t\t\"type\":\"int\"\n" +
                "\t\t\t\t\t\t}\n" +
                "\t\t\t\t\t],\n" +
                "\t\t\t\t\t\"defaultFS\":\"hdfs://ecs-f0cf-0001:1429\",\n" +
                "\t\t\t\t\t\"writeMode\":\"truncate\",\n" +
                "\t\t\t\t\t\"fieldDelimiter\":\",\",\n" +
                "\t\t\t\t\t\"fileType\":\"text\"\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"name\":\"hdfswriter\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "\t],\n" +
                "\t\"setting\":{\n" +
                "\t\t\"syncMeta\":\"true\",\n" +
                "\t\t\"errorLimit\":{\n" +
                "\t\t\t\"record\":\"100\"\n" +
                "\t\t},\n" +
                "\t\t\"useProcessor\":\"false\",\n" +
                "\t\t\"speed\":{\n" +
                "\t\t\t\"byte\":\"10240\",\n" +
                "\t\t\t\"record\":\"1000\",\n" +
                "\t\t\t\"channel\":\"1\"\n" +
                "\t\t},\n" +
                "\t\t\"advance\":{\n" +
                "\t\t\t\"mMemory\":\"1000m\"\n" +
                "\t\t}\n" +
                "\t}\n" +
                "}";

        Map<String,String> rwMaps = new HashMap<>();
        rwMaps.put("reader","mysqlreader");
        rwMaps.put("writer","hdfswriter");
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

package com.webank.wedatasphere.linkis.computation.client;

import com.webank.wedatasphere.linkis.common.conf.Configuration;
import com.webank.wedatasphere.linkis.computation.client.once.simple.SubmittableSimpleOnceJob;
import com.webank.wedatasphere.linkis.computation.client.utils.LabelKeyUtils;

import java.util.HashMap;
import java.util.Map;

public class SqoopOnceJobTest {
    private static String ExportSql = "export --connect jdbc:mysql://192.168.0.66:3306/linkis-bak --username scm --password scm_@casc2f --table linkis_task --num-mappers 1 --export-dir hdfs:///tmp/test.db/sqooptest2";
    private static String ImportSql = "import --connect jdbc:mysql://192.168.0.66:3306/authorization_test --username scm --password scm_@casc2f --table user_test --fields-terminated-by '\\t' --hive-import --hive-table default.test04 --hive-overwrite --num-mappers 1 ;";
    //
    public static void main(String[] args) throws InterruptedException {
        LinkisJobClient.config().setDefaultServerUrl("http://124.70.31.149:20088");
        Map<String,String> params = new HashMap<>();
        params.put("sqoop.mode","import");
        params.put("sqoop.args.connect","jdbc:mysql://192.168.0.66:3306/authorization_test");
        params.put("sqoop.args.username","scm");
        params.put("sqoop.args.password","scm_@casc2f");
        params.put("sqoop.args.table","user_test");
        params.put("sqoop.args.delete.target.dir","");
        params.put("sqoop.args.fields.terminated.by","\\t");
        params.put("sqoop.args.num.mappers","1");
        params.put("sqoop.args.hive.import","");
        params.put("sqoop.args.hive.database","default");
        params.put("sqoop.args.hive.table","test04");
        params.put("sqoop.args.hive.overwrite","");

        SubmittableSimpleOnceJob onceJob = LinkisJobClient.once().simple().builder().setCreateService("Sqoop-Test")
                .setMaxSubmitTime(300000)
                .addLabel(LabelKeyUtils.ENGINE_TYPE_LABEL_KEY(), "sqoop-1.4.7")
                .addLabel(LabelKeyUtils.USER_CREATOR_LABEL_KEY(), "hdfs-sqoop")
                .addLabel(LabelKeyUtils.ENGINE_CONN_MODE_LABEL_KEY(), "once")
                .addStartupParam(Configuration.IS_TEST_MODE().key(), true)
                .addExecuteUser("hdfs")
                .addJobContent("runType", "appconn")
                .addJobContent("sqoop-params", params)
                .addSource("jobName", "OnceJobTest")
                .build();
        onceJob.submit();
        System.out.println(onceJob.getId());

        onceJob.waitForCompleted();
        System.out.println(onceJob.getStatus());
        LinkisJobMetrics jobMetrics = onceJob.getJobMetrics();
        System.out.println(jobMetrics.getMetrics());

       // System.exit(0);
    }
}

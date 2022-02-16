package com.webank.wedatasphere.linkis.metadatamanager.service;

import com.webank.wedatasphere.linkis.common.conf.CommonVars;
import com.webank.wedatasphere.linkis.metadatamanager.common.domain.MetaColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlConnection implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SqlConnection.class);

    private static final CommonVars<String> SQL_DRIVER_CLASS =
            CommonVars.apply("wds.linkis.server.mdm.service.dameng.driver", "dm.jdbc.driver.DmDriver");

    private static final CommonVars<String> SQL_CONNECT_URL =
            CommonVars.apply("wds.linkis.server.mdm.service.dameng.url", "jdbc:dm://%s:%s/%s");

    private Connection conn;

    private ConnectMessage connectMessage;

    public SqlConnection(String host, Integer port,
                         String username, String password,
                         String database,
                         Map<String, Object> extraParams ) throws ClassNotFoundException, SQLException {
        connectMessage = new ConnectMessage(host, port, username, password, extraParams);
        conn = getDBConnection(connectMessage, database);
        //Try to create statement
        Statement statement = conn.createStatement();
        statement.close();
    }

    public List<String> getAllDatabases() throws SQLException {
//        java.util.List<java.lang.String> dataBaseName = new ArrayList<>();
//        Statement stmt = null;
//        ResultSet rs = null;
//        try{
//            stmt = conn.createStatement();
//            rs = stmt.executeQuery("SHOW DATABASES");
//            while (rs.next()){
//                dataBaseName.add(rs.getString(1));
//            }
//        } finally {
//            closeResource(null, stmt, rs);
//        }
//        return dataBaseName;
        throw new UnsupportedOperationException("dm数据库不能像mysql show databases来获取，应该是存在某个地方来获取的");
    }

//    public List<String> getAllTables(String database) throws SQLException {
//        List<String> tableNames = new ArrayList<>();
//        Statement stmt = null;
//        ResultSet rs = null;
//        try {
//            stmt = conn.createStatement();
//            rs = stmt.executeQuery("SHOW TABLES FROM `" + database + "`");
//            while (rs.next()) {
//                tableNames.add(rs.getString(1));
//            }
//            return tableNames;
//        } finally{
//            closeResource(null, stmt, rs);
//        }
//    }

    public List<String> getAllTables(String schema) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select TABLE_NAME from dba_tables where owner='" + schema + "'");
            while (rs.next()) {
                tableNames.add(rs.getString(1));
            }
            return tableNames;
        } finally{
            closeResource(null, stmt, rs);
        }
    }

    public List<MetaColumnInfo> getColumns(String database, String table) throws SQLException, ClassNotFoundException {
        List<MetaColumnInfo> columns = new ArrayList<>();
        String columnSql = "SELECT * FROM " + database +"." + table + " WHERE 1 = 2";
        PreparedStatement ps = null;
        ResultSet rs = null;
        ResultSetMetaData meta = null;
        try {
            List<String> primaryKeys = getPrimaryKeys(table);
            ps = conn.prepareStatement(columnSql);
            rs = ps.executeQuery();
            meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 1; i < columnCount + 1; i++) {
                MetaColumnInfo info = new MetaColumnInfo();
                info.setIndex(i);
                info.setName(meta.getColumnName(i));
                info.setType(meta.getColumnTypeName(i));
                if(primaryKeys.contains(meta.getColumnName(i))){
                    info.setPrimaryKey(true);
                }
                columns.add(info);
            }
        }finally {
            closeResource(null, ps, rs);
        }
        return columns;
    }

//    /**
//     * Get primary keys
//     * @param connection connection
//     * @param table table name
//     * @return
//     * @throws SQLException
//     */
//    private List<String> getPrimaryKeys(Connection connection, String table) throws SQLException {
//        ResultSet rs = null;
//        List<String> primaryKeys = new ArrayList<>();
//        try {
//            DatabaseMetaData dbMeta = connection.getMetaData();
//            rs = dbMeta.getPrimaryKeys(null, null, table);
//            while(rs.next()){
//                primaryKeys.add(rs.getString("column_name"));
//            }
//            return primaryKeys;
//        }finally{
//            if(null != rs){
//                closeResource(connection, null, rs);
//            }
//        }
//    }
    private List<String> getPrimaryKeys(/*Connection connection, */String table) throws SQLException {
        ResultSet rs = null;
        List<String> primaryKeys = new ArrayList<>();
//        try {
        DatabaseMetaData dbMeta = conn.getMetaData();
        rs = dbMeta.getPrimaryKeys(null, null, table);
        while(rs.next()){
            primaryKeys.add(rs.getString("column_name"));
        }
        return primaryKeys;
        /*}finally{
            if(null != rs){
                closeResource(connection, null, rs);
            }
        }*/
    }

    /**
     * close database resource
     * @param connection connection
     * @param statement statement
     * @param resultSet result set
     */
    private void closeResource(Connection connection,  Statement statement, ResultSet resultSet){
        try {
            if(null != resultSet && !resultSet.isClosed()) {
                resultSet.close();
            }
            if(null != statement && !statement.isClosed()){
                statement.close();
            }
            if(null != connection && !connection.isClosed()){
                connection.close();
            }
        }catch (SQLException e){
            LOG.warn("Fail to release resource [" + e.getMessage() +"]", e);
        }
    }

    @Override
    public void close() throws IOException {
        closeResource(conn, null, null);
    }

    /**
     * @param connectMessage
     * @param database
     * @return
     * @throws ClassNotFoundException
     */
    private Connection getDBConnection(ConnectMessage connectMessage, String database) throws ClassNotFoundException, SQLException {
        String extraParamString = connectMessage.extraParams.entrySet()
                .stream().map(e -> String.join("=", e.getKey(), String.valueOf(e.getValue())))
                .collect(Collectors.joining("&"));
        Class.forName(SQL_DRIVER_CLASS.getValue());
        String url = String.format(SQL_CONNECT_URL.getValue(), connectMessage.host, connectMessage.port, database);
        if(!connectMessage.extraParams.isEmpty()) {
            url += "?" + extraParamString;
        }
        try {
            return DriverManager.getConnection(url, connectMessage.username, connectMessage.password);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Connect message
     */
    private static class ConnectMessage{
        private String host;

        private Integer port;

        private String username;

        private String password;

        private Map<String, Object> extraParams;

        public ConnectMessage(String host, Integer port,
                              String username, String password,
                              Map<String, Object> extraParams){
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.extraParams = extraParams;
        }
    }

}

package com.ywf.until;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * ClassName HbaseUntil
 * 功能: hbase 工具类
 * Author YangWeiFeng
 * Date 2019/11/28 10:59
 * Version 1.0
 **/
public class HbaseUntil {
    public static Configuration configuration = null;
    public static HBaseAdmin hBaseAdmin = null;
    public static Connection connection = null;
    static{
        configuration = HBaseConfiguration.create();
        //配置zookeeper,配置一个就OK
        configuration.set("hbase.zookeeper.quorum", "xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            User user = User.create(UserGroupInformation.createRemoteUser("xxx"));
            connection =  ConnectionFactory.createConnection(configuration,user);
            hBaseAdmin = (HBaseAdmin)connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 遍历hbase命名空间
     * @throws IOException 异常
     */
    public static String getListNamespace() {
        StringBuffer stringBuffer = new StringBuffer();
        try {
            NamespaceDescriptor[] namespaceDescriptors = hBaseAdmin.listNamespaceDescriptors();
            for(NamespaceDescriptor namespaceDescriptor:namespaceDescriptors){
                stringBuffer.append(namespaceDescriptor);
            }
        }catch (Exception e){
           e.printStackTrace();
        }
       return stringBuffer.toString();
    }

    /**
     * 遍历指定命名空间的hbase表
     * @param namespace 命名空间
     * @throws Exception 异常
     */
    public static String listTables(String namespace){
        StringBuffer stringBuffer = new StringBuffer();
        try{
            //获取指定命名空间下的表
            TableName[] tables = hBaseAdmin.listTableNamesByNamespace(namespace);
            System.out.println(namespace + "下的表名：");
            for (TableName table:tables){
                stringBuffer.append(TableName.valueOf(table.getName()));
            }
            tables = hBaseAdmin.listTableNames();
            System.out.println("hbase所有表名：");
            for (TableName table:tables){
                System.out.println(table);
            }
        }catch (Exception e){
           e.printStackTrace();
        }
        return stringBuffer.toString();
    }

    /**
     * 全表扫描
     * @throws IOException 异常
     */
    public static void scanTableByName(String tableName) throws IOException {
        try {
            ResultScanner rs = connection.getTable(TableName.valueOf(tableName)).getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("rowKey: " + Bytes.toString(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + Bytes.toString(keyValue.getFamily()));
                    System.out.println("值:" + Bytes.toString(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 依据rowkey查询数据
     * @param rowKey 行键
     * @return
     */
    public static String queryTableByRowKey(String tableName, String rowKey){
        try {
            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询
            Result r = connection.getTable(TableName.valueOf(tableName)).get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
               return new String(keyValue.getValue(), "utf-8");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) throws IOException {
        //queryTableByRowKey("default:test2","00000000000000000000011895");
    }

}

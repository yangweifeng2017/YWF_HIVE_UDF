package com.ywf.udf;

import com.ywf.until.HbaseUntil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * ClassName HbaseQueryByKeyUdf
 * 功能: 在hive中通过函数指定rowkey查询hbase中的数据
 * Author YangWeiFeng
 * Date 2019/11/28 10:21
 * Version 1.0
 **/
public class HbaseQueryByKeyUdf extends UDF {
    /**
     * 通过rowkey查询hbase中的数据
     *
     * @return values
     */
    public String evaluate(String tableName, String rowKey) throws IOException {
        return HbaseUntil.queryTableByRowKey(tableName, rowKey);
    }

}

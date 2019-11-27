package com.ywf.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * ClassName Md5Udf
 * 功能: MD5 udf函数
 * Author YangWeiFeng
 * Date 2019/11/27 10:34
 * Version 1.0
 * 部署方式:
   永久生效
   将jar包上传到 /XXX/XXX/XXX
   在hive命令行中执行: CREATE FUNCTION XXX_md5 AS 'com.XXX.udf.Md5Udf' using jar 'viewfs://c9//XXX/XXX/XXX/XXXX_udf_warehouse-1.0-jar-with-dependencies.jar';
   如需更改，只需要更改相应的jar包，重新上传即可
 **/
public class Md5Udf extends UDF {
    /**
     * 执行器
     * @param args 传递参数值
     * @param type Boolean true为大写 false为小写
     * @return md5
     */
    public String evaluate(String args,Boolean type) {
        String md5 =  DigestUtils.md5Hex(args);
        if (type){
            return md5.toUpperCase();
        }else {
            return md5;
        }
    }
    public static void main(String[] args) {
        Md5Udf md5Udf = new Md5Udf();
        System.out.println(md5Udf.evaluate("111",false));
    }
}
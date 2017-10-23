package com.products;

import com.products.lineage.EtlOperation;
import com.products.util.JsonUtil;
import com.products.util.PropertyFileUtil;

import org.apache.hadoop.util.StringUtils;
import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 *  java -jar  hive-parser.jar /home/yunxiao/hello/app.properties
 */
public class App {
    public static void main(String[] args) throws Exception {

        System.out.println("参数:" + StringUtils.join(",", args));

        if(args.length > 0){
            String path = args[0] ;
            PropertyFileUtil.init(path);
        }else{
            PropertyFileUtil.init("/app.properties");
        }

        Boolean testFlag  = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.test")) ; //是否测试

        EtlOperation operation = new EtlOperation() ;

        if (testFlag) {
            // 是否测试
            operation.parseTestSql() ;
        } else {
            operation.parse_beeper_data_warehouse_sql() ;
            operation.saveToFileForBadSql() ;
        }
    }
}
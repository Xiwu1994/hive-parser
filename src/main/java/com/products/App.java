package com.products;

import com.products.lineage.EtlOperation;
import com.products.util.JsonUtil;
import com.products.util.PropertyFileUtil;

import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;

public class App {
    public static void main(String[] args) throws Exception {
        EtlOperation operation = new EtlOperation() ;
        Boolean testFlag  = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.test")) ; //是否测试
        if (testFlag) {
            // 是否测试
            operation.parseTestSql() ;
        } else {
            operation.parse_beeper_data_warehouse_sql() ;
            operation.saveToFileForBadSql() ;
        }
    }
}
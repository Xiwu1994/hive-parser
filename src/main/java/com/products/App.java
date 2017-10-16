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
            //operation.parseSqlFile("/Users/liebaomac/PhpstormProjects/beeper_data_warehouse/job/sql/app_beeper/app_beeper_trans_event_index_statistics_personal.sql");
//            operation.parseSql(
//                    "insert overwrite table ttt.ttt " +
//                    "select t1.xx,t2.yy " +
//                    "from d1.aa t1      " +
//                    "left join          " +
//                    "(                  " +
//                    "   select yy       " +
//                    "   from d1.bb      " +
//                    "   where id>3      " +
//                    ")t2                " +
//                    "on t1.id = t2.id   "
//            );
        } else {
            operation.parse_beeper_data_warehouse_sql() ;
            operation.saveToFileForBadSql() ;
        }
    }
}
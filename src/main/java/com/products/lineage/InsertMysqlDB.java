package com.products.lineage;

import com.products.bean.ColLine;
import com.products.bean.SQLResult;
import com.products.util.MysqlUtil;
import com.products.util.PropertyFileUtil;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InsertMysqlDB {

    public void cleanDB() {
        MysqlUtil mysqlUtil = new MysqlUtil(MysqlUtil.DB_TYPE.TASK);
        try {
            mysqlUtil.doDelete("truncate table table1");
            mysqlUtil.doDelete("truncate table table2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertDB(SQLResult oneResult) throws Exception {
        /*
        * 将解析sql后的的结果 录入mysql
        * */

        // 1、 输出 <表名、依赖的表名、依赖的字段>
        MysqlUtil dbUtil = new MysqlUtil(MysqlUtil.DB_TYPE.TASK);
        // a.初始化
        HashMap<String, List> dependenceTableColumn = new HashMap<String, List>();
        for (String dependenceTable: oneResult.getInputTables()) {
            List columnList = new LinkedList<String>();
            dependenceTableColumn.put(dependenceTable, columnList);
        }
        // b. 写数据
        for (String dependenceColumn : oneResult.getDependenceColunmList()) {
            String dependenceTable = dependenceColumn.split("\\.")[0] + "." + dependenceColumn.split("\\.")[1];
            if (dependenceTableColumn.containsKey(dependenceTable)) {
                dependenceTableColumn.get(dependenceTable).add(dependenceColumn.split("\\.")[2]);
            } else {
                System.out.print("BADDDDDDDDD!!! dependenceTable: " + dependenceTable + "\n");
            }
        }
        // c. 组装sql
        String tableName = "";
        for (String outPutTable : oneResult.getOutputTables()) {
            tableName = outPutTable;
        }
        for (Map.Entry<String, List> entry : dependenceTableColumn.entrySet()) {
            if (entry.getKey().length() == 0) {
                String dependenceColumn = "";
                String insertSQL = "insert into table1(tableName, dependenceTableName, dependenceColumnName) values " +
                        "('" + tableName + "','" + entry.getKey() + "','" + dependenceColumn + "')";
                dbUtil.doInsert(insertSQL);
            } else {
                for (Object dependenceColumn : entry.getValue()) {
                    String insertSQL = "insert into table1(tableName, dependenceTableName, dependenceColumnName) values " +
                            "('" + tableName + "','" + entry.getKey() + "','" + dependenceColumn + "')";
                    dbUtil.doInsert(insertSQL); //入库
                }
            }
        }

        // 2、 输出 <表名、字段名、字段怎么计算>
        for (ColLine colLine : oneResult.getColLineList()) {
            String columnName = colLine.getToName();
            if (columnName == null) {
                columnName = colLine.getToNameParse();
            }
            String calculateMethod = colLine.getColCondition();
            if (calculateMethod == null) {
                calculateMethod = "null";
            }
            String insertSQL = "insert into table2(tableName, columnName, calculateMethod) values " +
                    "('" + tableName + "','" + columnName + "','" + calculateMethod.replace("'", "`") + "')";
            dbUtil.doInsert(insertSQL); //入库
        }
    }
}

package com.products.lineage;

import com.products.bean.ColLine;
import com.products.bean.SQLResult;
import com.products.parse.LineParser;
import com.products.util.*;

import java.util.List;

public class Neo4jDB {
    static final String url = "bolt://localhost:7687";
    static final Neo4jUtil neo4jUtil = new Neo4jUtil(url, "neo4j", "root");

    public void insertDB(SQLResult oneResult) throws Exception {
        /*
        * 将sql解析后的结果 录入neo4j
        * */

        String outputTable = null;
        for (String tableName: oneResult.getOutputTables()) {
            outputTable = tableName;
        }
        if (!neo4jUtil.isExistTableNodes(outputTable)) { // 将输出表入库
            neo4jUtil.createTableNode(outputTable);
        }
        for (String tableName: oneResult.getInputTables()) { // 将输入表入库并且添加依赖关系
            if (tableName.equals(outputTable)) {continue;}
            if (!neo4jUtil.isExistTableNodes(tableName)) {
                neo4jUtil.createTableNode(tableName);
            }
            if (!neo4jUtil.isExistRelationshipBetweenTables(outputTable, tableName)) {
                neo4jUtil.createRelationshipBetweenTables(outputTable, tableName);
            }
        }

        for (ColLine colLine: oneResult.getColLineList()) {
            String columnFullName = colLine.getToName();
            if(columnFullName == null) {
                String columnSimpleName = colLine.getToNameParse();
                columnFullName = outputTable + "." + columnSimpleName;
            }
            if (!neo4jUtil.isExistColumnNodes(columnFullName)) {
                neo4jUtil.createColumnNode(columnFullName);
                //TODO 字段的计算方法
                //字段之间的依赖关系
                for (String depColumnName: colLine.getFromNameSet()) {
                    if(!neo4jUtil.isExistColumnNodes(depColumnName)) {
                        neo4jUtil.createColumnNode(depColumnName);
                        neo4jUtil.createRelationshipBetweenColumns(columnFullName, depColumnName);
                    } else if (!neo4jUtil.isExistRelationshipBetweenColumns(columnFullName, depColumnName)) {
                        neo4jUtil.createRelationshipBetweenColumns(columnFullName, depColumnName);
                    }
                }
                //TODO 字段和表之间所属关系(需要做吗？)
            }
        }
    }

    public void parseSqlFile(String filePath) throws Exception {
        /*
        * 解析某一个sql文件 有写入操作的sql
        * */
        LineParser parser = new LineParser();
        String sqlList = FileUtil.read(filePath);
        System.out.print("Begin: " + filePath + "\n");
        for (String sql : sqlList.split(";")) {
            if (! filePath.contains("discard")) {
                if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                    sql = sql.replace("${", "\"").replace("}", "\"");//.replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                    System.out.printf(sql + "\n");
                    try {
                        List<SQLResult> result = parser.parse(sql); // 解析SQL
                        System.out.printf(JsonUtil.objectToJson(result) + "\n");
                        for (SQLResult oneResult : result) {
                            insertDB(oneResult);
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        System.out.print("End:   " + filePath + "\n\n");
    }

    public static void main(String[] args) {
        Neo4jDB neo4jDB = new Neo4jDB();
        //neo4jUtil.cleanDB();
        try {
            neo4jDB.parseSqlFile(PropertyFileUtil.getProperty("local_file_path.test_sql"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
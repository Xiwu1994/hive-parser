package com.products;
import com.products.bean.ColLine;
import com.products.bean.RealationShip;
import com.products.bean.SQLResult;
import com.products.parse.LineParser;
import com.products.util.*;
import org.apache.commons.lang.StringUtils;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.io.fs.FileUtils;
import org.yaml.snakeyaml.Yaml;

import org.neo4j.driver.v1.*;
import static org.neo4j.driver.v1.Values.parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class App {
    private static Integer TEST_FLAG = 0; //是否测试
    private static Integer INSERT_NEO4J = 1;
    private static Integer INSERT_MYSQL = 0;

    public enum RelTypes implements RelationshipType {
        depColumn, depTable
    }
    public enum MyLables implements Label {
        Table, Column
    }

    private static final String filePath = "/Users/liebaomac/IdeaProjects/neo4j/packaging/standalone/target/neo4j-community-3.3.0-SNAPSHOT/data/databases/lineage_analysis.db";
    static {
        try {
            FileUtils.deleteRecursively(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(filePath));

    private static void insert_into_neo4j(SQLResult oneResult) throws Exception {
        try (Transaction tx = graphDb.beginTx()) {
            String outputTable = "";
            for (String tableName: oneResult.getOutputTables()) {
                outputTable = tableName;
            }
            IndexManager indexManager = graphDb.index();
            Index<Node> tableIndex =indexManager.forNodes("tables");
            // 1、创建表节点
            Node tableNode = graphDb.findNode(MyLables.Table, "name", outputTable);
            if (tableNode == null) {
                tableNode = graphDb.createNode();
                tableNode.setProperty("name", outputTable);
                tableNode.addLabel(MyLables.Table);
                tableIndex.add(tableNode, "name", outputTable);
            }
            /*
            for (ColLine colLine : oneResult.getColLineList()) { //处理表自己的字段 <不走这里了，字段处理的时候会有问题>
                String columnName = colLine.getToName();
                if (columnName == null) {
                    columnName = outputTables + "." + colLine.getToNameParse();
                }
                Node columnNode = graphDb.findNode(MyLables.Column, "name", columnName);
                if (columnNode == null) {
                    // 1、创建节点
                    columnNode = graphDb.createNode();
                    columnNode.setProperty("name", columnName);
                    columnNode.setProperty("table", outputTables);
                    columnNode.addLabel(MyLables.Column);
                }
                // 2、创建关系
                tableNode.createRelationshipTo(columnNode, RelTypes.has);
                columnNode.createRelationshipTo(tableNode, RelTypes.belong);
            }
            */
            // 2、处理依赖表
            for (String tableName: oneResult.getInputTables()) {
                if (tableName.equals(outputTable)) {continue;}
                Node depTableNode = graphDb.findNode(MyLables.Table, "name", tableName);
                // a、创建节点
                if (depTableNode == null) {
                    depTableNode = graphDb.createNode();
                    depTableNode.setProperty("name", tableName);
                    depTableNode.addLabel(MyLables.Table);
                    tableIndex.add(depTableNode, "name", tableName);
                }
                // b. 判断关系是否存在
                Integer table_rel_flag = 0;
                for (Relationship rel : tableNode.getRelationships(RelTypes.depTable)) {
                    if (rel.getProperty("direct").equals(outputTable+"->"+tableName)) {
                        table_rel_flag = 1;
                    }
                }
                // c、创建关系 <表节点之间依赖和被依赖>
                if(table_rel_flag == 0) {
                    Relationship relDepTable = tableNode.createRelationshipTo(depTableNode, RelTypes.depTable);
                    relDepTable.setProperty("direct", outputTable+"->"+tableName);
                }
            }
            // 3、处理依赖表的具体字段
            for (String columnName: oneResult.getDependenceColunmList()) {
                String depTableName = columnName.split("\\.")[0] + "." + columnName.split("\\.")[1];
                if (depTableName.equals(outputTable)) {continue;}
                String depColumnName = columnName;
                Node depColumnNode = graphDb.findNode(MyLables.Column, "name", depColumnName);
                // a、创建节点
                if(depColumnNode == null) {
                    depColumnNode = graphDb.createNode();
                    depColumnNode.setProperty("name", depColumnName);
                    depColumnNode.setProperty("table", depTableName);
                    depColumnNode.addLabel(MyLables.Column);
                }
                // b、 判断关系是否存在
                Integer column_rel_flag = 0;
                for (Relationship rel : tableNode.getRelationships(RelTypes.depColumn)) {
                    if (rel.getProperty("direct").equals(outputTable+"->"+depColumnName)) {
                        column_rel_flag = 1;
                    }
                }
                // c、创建关系 <表和字段节点之间依赖和被依赖>
                if (column_rel_flag == 0) {
                    Relationship relDepColumn = tableNode.createRelationshipTo(depColumnNode, RelTypes.depColumn);
                    relDepColumn.setProperty("direct", outputTable+"->"+depColumnName);
                }
            }
            tx.success();
        }
    }

    private  static void insert_into_mysql(SQLResult oneResult) throws Exception {
        DBUtil dbUtil = new DBUtil(DBUtil.DB_TYPE.TASK);
        // 1、 输出 <表名、依赖的表名、依赖的字段>
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
//                            System.out.print("insertSQL: " + insertSQL + "\n");  //////
                    dbUtil.doInsert(insertSQL);
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

            dbUtil.doInsert(insertSQL);
        }
    }

    private static void parse_sql(String sql) throws Exception {
        PropertyFileUtil.init();
        LineParser parser = new LineParser();
        List<SQLResult> result = parser.parse(sql); // 解析SQL

        for (SQLResult oneResult : result) {
            if (INSERT_NEO4J == 1) {
                insert_into_neo4j(oneResult);
            }
            if (INSERT_MYSQL == 1) {
                 insert_into_mysql(oneResult);
            }
        }
//        System.out.println(JsonUtil.objectToJson(result));
    }

    public static void main(String[] args) throws Exception {
//        String sql = "select driver_id from odps.heartbeat";
//        parse_sql(sql);
        if (TEST_FLAG == 0) {
            LinkedList bad_sql_list = new LinkedList<String>();

            // 1、sql文件
            LinkedList file_path_list = traverseFolder.traverseFolder("/Users/liebaomac/PhpstormProjects/beeper_data_warehouse/job/sql");
            for (Object file_path : file_path_list) {
                String sql_list = FileUtil.read(file_path.toString());

                for (String sql : sql_list.split(";")) {
                    if (! file_path.toString().contains("discard")) {
                        if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                            sql = sql.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                            try {
                                System.out.print("Begin: " + file_path + "\n");
                                parse_sql(sql);
                                System.out.print("End:   " + file_path + "\n\n");
                        } catch(Exception e){
                            bad_sql_list.add(sql);
                        }
                    }
                    }
                }
            }

            // 2、yml文件
            LinkedList file_path_list_2 = traverseFolder.traverseFolder("/Users/liebaomac/PhpstormProjects/beeper_data_warehouse/job/script");
            for (Object file_path : file_path_list_2) {
                Yaml yaml = new Yaml();
                try {
                    Map res = yaml.loadAs(new FileInputStream(file_path.toString()), Map.class);
                    List<Map<String,Object>> steps = (List)(res.get("steps")) ;
                    for(Map<String,Object> step :steps){

                        if(step.containsKey("type") && step.get("type").equals("hive")){
                            List<Map<String,Object>> sqls = (List<Map<String,Object>>)step.get("sqls") ;
                            for(Map<String,Object> yaml_sql: sqls) {
                                Map<String,Object> sqlMap = (Map<String,Object>)yaml_sql.get("sql") ;

                                String path = (String)(sqlMap.get("path"));
                                String value = (String)(sqlMap.get("value"));
                                if(StringUtils.isNotEmpty(value)){
                                    if (value.contains("insert") || value.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                                        String sql = value.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                                        //System.out.print(sql + "\n\n");
                                        try {
                                            System.out.print("Begin: " + file_path + "\n");
                                            parse_sql(sql);
                                            System.out.print("End:   " + file_path + "\n\n");
                                        } catch (Exception e) {
                                            bad_sql_list.add(sql);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("ERROR. file_path" + file_path.toString());
                    continue;
                }
            }

            String bad_sql_string = "";
            String bad_sql_file_path = "/Users/liebaomac/IdeaProjects/hive-parser/src/main/java/com/products/bad_sql.txt";
            for (Object bad_sql: bad_sql_list) {
                bad_sql_string += bad_sql.toString() + ";\n\n";
            }
            FileUtil.createFile(bad_sql_file_path, bad_sql_string);

        } else {

            String sql_list = FileUtil.read("/Users/liebaomac/IdeaProjects/hive-parser/src/main/java/com/products/xxx.txt");
            for (String sql : sql_list.split(";")) {
                if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                    sql = sql.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                    parse_sql(sql);
                }
            }
        }
    }
}
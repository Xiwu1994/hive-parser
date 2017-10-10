package com.products.lineage;

import com.products.bean.SQLResult;
import com.products.util.PropertyFileUtil;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;

public class InsertNeo4jDB {
    public enum RelTypes implements RelationshipType {
        depColumn, depTable
    }
    public enum MyLables implements Label {
        Table, Column
    }
    private static final String filePath = PropertyFileUtil.getProperty("neo4j.db.path");
    private static final GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(filePath));

    public void cleanDB() {
        try {
            FileUtils.deleteRecursively(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertDB(SQLResult oneResult) throws Exception {
        /*
        * 将sql解析后的结果 录入neo4j
        * */
        try (Transaction tx = graphDb.beginTx()) {
            String outputTable = null;
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

}

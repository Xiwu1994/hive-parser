package com.products;

import com.products.bean.SQLResult;
import com.products.parse.LineParser;
import com.products.util.FileUtil;
import com.products.util.PropertyFileUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by liebaomac on 17/9/22.
 */
public class AppNeo {


    public static void  main(String args[]) throws  Exception {

        SQLResult oneResult = new SQLResult() ;

        Set<String> outputTables1 = new HashSet<String>() ;
        outputTables1.add("db1.t_1") ;


        Set<String> inputTables = new HashSet<String>() ;
        outputTables1.add("db1.t_2") ;

        Set<String> columnList = new HashSet<String>() ;

        columnList.add("db1.t_1.c1") ;
        columnList.add("db2.t_1.c2") ;

        oneResult.setOutputTables(outputTables1);

        oneResult.setInputTables(inputTables);

        oneResult.setDependenceColunmList(columnList);



        System.out.println(oneResult);

        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        String filePath = "/Users/liebaomac/IdeaProjects/neo4j/packaging/standalone/target/neo4j-community-3.3.0-SNAPSHOT/data/databases/lineage_analysis.db";
//        String filePath = "/Users/liebaomac/lineage_analysis.db";
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase(new File(filePath));

        System.out.println(graphDb) ;

        try (Transaction tx = graphDb.beginTx()) {
            // Perform DB operations
            String outputTables = ""; // insert的tableName
            for (String tableName: oneResult.getOutputTables()) {
                outputTables = tableName;
            }
            Node tableNode = graphDb.createNode();
            tableNode.setProperty("name", outputTables);

            for (String tableName: oneResult.getInputTables()) {
                // 1、创建节点
                Node depTableNode = graphDb.createNode();
                depTableNode.setProperty("name", tableName);

                // 2、创建关系
                Relationship relTable = tableNode.createRelationshipTo(depTableNode, App.RelTypes.depColumn);
                relTable.setProperty("type", "table");
            }

            for (String columnName: oneResult.getDependenceColunmList()) {
                // 1、创建节点
                String depTableName = columnName.split("\\.")[0] + "." + columnName.split("\\.")[1];
                String depColumnName = columnName.split("\\.")[2];
                Node depColumnNode = graphDb.createNode();
                depColumnNode.setProperty("name", depColumnName);
                depColumnNode.setProperty("table", depTableName);

                // 2、创建关系
                Relationship relColumn = tableNode.createRelationshipTo(depColumnNode, App.RelTypes.depColumn);
                relColumn.setProperty("type", "column");
            }
            tx.success();
        }

    }
}

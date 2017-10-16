package com.products.util;

import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jUtil {

    private final Driver driver;
    public Neo4jUtil(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    public void createTableNode(String tableName) {
        /*
        * 创建表节点
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("CREATE (n:Table { name: {tableName} })", parameters("tableName", tableName));
                tx.success();
            }
        }
    }

    public void createColumnNode(String columnName) {
        /*
        * 创建字段节点
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("CREATE (n:Column { name: {columnName} })", parameters("columnName", columnName));
                tx.success();
            }
        }
    }

    public void createRelationshipBetweenTables(String originTableName, String destTableName) {
        /*
        * 创建表之间的关系
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("MATCH (a:Table),(b:Table)" +
                       "WHERE a.name = {originTableName} AND b.name = {destTableName}" +
                       "CREATE (a)-[r:depTable]->(b)",
                       parameters("originTableName", originTableName, "destTableName", destTableName));
                tx.success();
            }
        }
    }

    public void createRelationshipBetweenColumns(String originColumnName, String destColumnName) {
        /*
        * 创建字段之间的关系
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("MATCH (a:Column),(b:Column)" +
                                "WHERE a.name = {originColumnName} AND b.name = {destColumnName}" +
                                "CREATE (a)-[r:depColumn]->(b)",
                        parameters("originColumnName", originColumnName, "destColumnName", destColumnName));
                tx.success();
            }
        }
    }


    public boolean isExistColumnNodes(String propertyName) {
        return isExistNodes("Column", "name", propertyName);
    }

    public boolean isExistTableNodes(String propertyName) {
        return isExistNodes("Table", "name", propertyName);
    }

    public boolean isExistNodes(String labelName, String propertyKey, String propertyName) {
        /*
        * 判断节点是否存在
        * */
        StatementResult result;
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                String cypher = String.format("MATCH (a:%s) WHERE a.%s = {propertyName} RETURN a", labelName, propertyKey);
                result = tx.run(cypher, parameters("propertyName", propertyName));
                tx.success();
            }
        }
        if (result.hasNext()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isExistRelationshipBetweenTables(String originTableName, String destTableName) {
        /*
        * 判断表之间关系是否存在
        * */
        StatementResult result;
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                result = tx.run("MATCH (a:Table)-[r:depTable]->(b:Table)" +
                       "WHERE a.name = {originTableName} AND b.name = {destTableName}" +
                       "RETURN r",
                       parameters("originTableName", originTableName, "destTableName", destTableName));
                tx.success();
            }
        }
        if (result.hasNext()) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isExistRelationshipBetweenColumns(String originColumnName, String destColumnName) {
        /*
        * 判断字段之间关系是否存在
        * */
        StatementResult result;
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                result = tx.run("MATCH (a:Column)-[r:depColumn]->(b:Column)" +
                                "WHERE a.name = {originColumnName} AND b.name = {destColumnName}" +
                                "RETURN r",
                        parameters("originColumnName", originColumnName, "destColumnName", destColumnName));
                tx.success();
            }
        }
        if (result.hasNext()) {
            return true;
        } else {
            return false;
        }
    }

    public void cleanDB() {
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("MATCH (n) DETACH DELETE n");
                tx.success();
            }
        }
    }

    public void createIndexForTable(String propertyKey) {
        createIndex("Table", propertyKey);
    }

    public void createIndexForColumn(String propertyKey) {
        createIndex("Column", propertyKey);
    }

    public void createIndex(String labelName, String propertyKey) {
        /*
        * 创建索引
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("CREATE INDEX ON:" + labelName + "(" + propertyKey + ")");
                tx.success();
            }
        }
    }

    public void deleteIndex(String labelName, String propertyKey) {
        /*
        * 删除索引
        * */
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("DROP INDEX ON:" + labelName + "(" + propertyKey + ")");
                tx.success();
            }
        }
    }

    public void close() throws Exception
    {
        /*
        * 关闭driver连接
        * */
        driver.close();
    }

    public static void main(String[] args) {
        String url = "bolt://localhost:7687";
        Neo4jUtil neo4jUtil = new Neo4jUtil(url, "neo4j", "root");
        if(neo4jUtil.isExistTableNodes("t1")){
            System.out.printf("11");
        }
    }
}

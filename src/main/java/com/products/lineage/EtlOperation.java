package com.products.lineage;

import com.products.bean.SQLResult;
import com.products.parse.LineParser;
import com.products.util.FileUtil;
import com.products.util.JsonUtil;
import com.products.util.PropertyFileUtil;
import com.products.util.traverseFolder;
import org.apache.commons.lang.StringUtils;
import org.neo4j.io.fs.FileUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EtlOperation {

    private boolean isInsertMySQL ;
    private boolean isInsertNeo4j ;
    private InsertMysqlDB insertMysqlDB;
    private InsertNeo4jDB insertNeo4jDB;

    private List<String> badSqls = new ArrayList<>() ;

    public EtlOperation(){
        Boolean insertMySQLFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.insert.mysql.flag")) ;
        Boolean insertNeo4jFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.insert.neo4j.flag")) ;
        Boolean cleanMySQLFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.clean.mysql.database")) ; // 清理mysql table
        Boolean cleanNeo4jFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.clean.neo4j.database")) ; // 清理neo4j table
        if(cleanNeo4jFlag) {
            cleanDBNeo4j() ;
        }
        this.isInsertMySQL = insertMySQLFlag ;
        this.isInsertNeo4j = insertNeo4jFlag ;
        if(this.isInsertNeo4j) {
            this.insertNeo4jDB = new InsertNeo4jDB();
        }
        if(this.isInsertMySQL) {
            this.insertMysqlDB = new InsertMysqlDB();
        }
        if(cleanMySQLFlag) {
            this.insertMysqlDB.cleanDB() ;
        }
    }

    public void cleanDBNeo4j() {
        try {
            String filePath = PropertyFileUtil.getProperty("neo4j.db.path");
            FileUtils.deleteRecursively(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseSql(String sql) throws Exception {
        /*
        * 解析sql + 入库(mysql OR neo4j)
        * */
        LineParser parser = new LineParser();
        List<SQLResult> result = parser.parse(sql); // 解析SQL

        System.out.printf(JsonUtil.objectToJson(result) + "\n");

        if (this.isInsertNeo4j) {
            for (SQLResult oneResult : result) {
                this.insertNeo4jDB.insertDB(oneResult);
            }
        }
        if (this.isInsertMySQL) {
            for (SQLResult oneResult : result) {
                this.insertMysqlDB.insertDB(oneResult);
            }
        }
    }

    public List<String> getSqlFromYaml(String filePath) throws Exception {
        /*
        * 获取yaml文件中的sql
        * 注: 一个yaml文件可能多有个sql，所以返回List
        * */
        List<String> sqlList = new ArrayList<>();
        if (filePath.contains("discard")) {
            return sqlList;
        }
        Yaml yaml = new Yaml();
        try {
            Map res = yaml.loadAs(new FileInputStream(filePath), Map.class);
            List<Map<String, Object>> steps = (List) (res.get("steps"));
            for (Map<String, Object> step : steps) {
                if (step.containsKey("type") && step.get("type").equals("hive")) {
                    List<Map<String, Object>> sqls = (List<Map<String, Object>>) step.get("sqls");
                    for (Map<String, Object> yaml_sql : sqls) {
                        Map<String, Object> sqlMap = (Map<String, Object>) yaml_sql.get("sql");
                        //String path = (String) (sqlMap.get("path"));
                        String value = (String) (sqlMap.get("value"));
                        sqlList.add(value);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("ERROR. yaml_file_path" + filePath);
        }
        return sqlList;
    }

    public void parseYamlFile(String filePath) throws Exception {
        /*
        * 解析某一个yaml文件中的 有写入操作的sql
        * */
        System.out.print("Begin: " + filePath + "\n");
        for (String value: getSqlFromYaml(filePath)) {
            if(StringUtils.isNotEmpty(value)) {
                if (value.contains("insert") || value.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                    String sql = value.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                    try {
                        parseSql(sql);
                    } catch (Exception e) {
                        badSqls.add(sql);
                    }
                }
            }
        }
        System.out.print("End:   " + filePath + "\n\n");
    }

    public void parseScriptPath() throws Exception {
        /*
        * 解析script目录下yaml文件中 有写入操作的sql
        * */
        String beeperDataWarehouseScriptPath = PropertyFileUtil.getProperty("beeper_data_warehouse.script.path");
        LinkedList filePathList = traverseFolder.traverseFolder(beeperDataWarehouseScriptPath);
        for (Object filePath : filePathList) {
            parseYamlFile(filePath.toString());
        }
    }

    public void parseSqlFile(String filePath) throws Exception {
        /*
        * 解析某一个sql文件 有写入操作的sql
        * */
        String sqlList = FileUtil.read(filePath);
        System.out.print("Begin: " + filePath + "\n");
        for (String sql : sqlList.split(";")) {
            if (! filePath.contains("discard")) {
                if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                    sql = sql.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                    try {
                        parseSql(sql);
                    } catch(Exception e){
                        badSqls.add(sql);
                    }
                }
            }
        }
        System.out.print("End:   " + filePath + "\n\n");
    }

    public void parseSqlPath() throws Exception {
        /*
        * 解析sql目录下 有写入操作的sql
        * */
        String beeperDataWarehouseSqlPath = PropertyFileUtil.getProperty("beeper_data_warehouse.sql.path");
        LinkedList filePathList = traverseFolder.traverseFolder(beeperDataWarehouseSqlPath);
        for (Object filePath : filePathList) {
            parseSqlFile(filePath.toString());
        }
    }

    public void parse_beeper_data_warehouse_sql() throws Exception {
        /*
        * 处理仓库代码中有写入操作的sql
        * */

        parseSqlPath(); // 1.处理sql目录下的sql

        parseScriptPath(); // 2.处理script目录下的sql
    }

    public void saveToFileForBadSql() throws Exception {
        /*
        * 将解析失败的sql保存到本地
        * */
        String badSqlString= null;
        String badSqlFilePath = PropertyFileUtil.getProperty("local_file_path.bad_sql");
        for (Object badSql: badSqls) {
            badSqlString += badSql.toString() + ";\n\n";
        }
        FileUtil.createFile(badSqlFilePath, badSqlString);
    }

    public void parseTestSql() throws Exception {
        String sqlList = FileUtil.read(PropertyFileUtil.getProperty("local_file_path.test_sql"));
        for (String sql : sqlList.split(";")) {
            if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                sql = sql.replace("${", "\"").replace("}", "\"").replace("lateral view", "-- lateral view").replace("LATERAL VIEW", "-- LATERAL VIEW");
                parseSql(sql);
            }
        }
    }

    public List<String> getBadSqls() {
        return badSqls;
    }
}

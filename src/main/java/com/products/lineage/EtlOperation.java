package com.products.lineage;

import com.products.bean.SQLResult;
import com.products.parse.LineParser;
import com.products.util.FileUtil;
import com.products.util.JsonUtil;
import com.products.util.PropertyFileUtil;
import com.products.util.traverseFolder;
import org.apache.commons.lang.StringUtils;
import org.apache.htrace.fasterxml.jackson.databind.annotation.JacksonStdImpl;
import org.yaml.snakeyaml.Yaml;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EtlOperation {

    static final boolean debugFlag = false;

    private boolean isInsertMySQL ;
    private boolean isInsertNeo4j ;
    private InsertMysqlDB insertMysqlDB;
    private Neo4jDB neo4jDB;

    private List<String> badSqls = new ArrayList<>() ;

    public EtlOperation(){
        Boolean insertMySQLFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.insert.mysql.flag")) ;
        Boolean insertNeo4jFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.insert.neo4j.flag")) ;
        Boolean cleanMySQLFlag = Boolean.parseBoolean(PropertyFileUtil.getProperty("app.clean.mysql.database")) ; // 清理mysql table
        this.isInsertMySQL = insertMySQLFlag ;
        this.isInsertNeo4j = insertNeo4jFlag ;
        if(this.isInsertNeo4j) {
            this.neo4jDB = new Neo4jDB();
        }
        if(this.isInsertMySQL) {
            this.insertMysqlDB = new InsertMysqlDB();
        }
        if(cleanMySQLFlag) {
            this.insertMysqlDB.cleanDB() ;
        }
    }

    public void parseSql(String sql) throws Exception {
        /*
        * 解析sql + 入库(mysql OR neo4j)
        * */
        LineParser parser = new LineParser();
        List<SQLResult> result = parser.parse(sql); // 解析SQL
        //System.out.println(JsonUtil.objectToJson(result));

        if (this.isInsertNeo4j) {
            for (SQLResult oneResult : result) {
                if (debugFlag) {
                    System.out.printf("insert neo4j db \n");
                }
                this.neo4jDB.insertDB(oneResult);
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
        if (! filePath.contains("discard") && ! filePath.contains("patch_up_historical_data")) {
            for (String value : getSqlFromYaml(filePath)) {
                if (StringUtils.isNotEmpty(value)) {
                    if (value.contains("insert") || value.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                        String sql = value.replace("${", "\"").replace("}", "\"").replace("\\\"", " ");
                        System.out.print("Begin: " + filePath + "\n");
                        try {
                            parseSql(sql);
                        } catch (Exception e) {
                            badSqls.add(sql);
                        }
                        System.out.print("End:   " + filePath + "\n\n");
                    }
                }
            }
        }
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
        if (! filePath.contains("discard") && ! filePath.contains("patch_up_historical_data")) {
            System.out.print("Begin: " + filePath + "\n");
            String sqlList = FileUtil.read(filePath);
            for (String sql : sqlList.split(";")) {
                if (sql.contains("insert") || sql.trim().replace("\n", " ").matches("create table.*as\\s.*")) {
                    sql = sql.replace("${", "'").replace("}", "'").replace("\\\"", " ");
                    try {
                        parseSql(sql);
                    } catch (Exception e) {
                        badSqls.add(sql);
                    }
                }
            }
            System.out.print("End:   " + filePath + "\n\n");
        }
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
        this.neo4jDB.cleanDB();

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
        this.neo4jDB.cleanDB();
        for (String sql : sqlList.split(";")) {
            sql = sql.replace("${", "'").replace("}", "'").replace("\\\"", " ");
            parseSql(sql);
        }
    }

    public List<String> getBadSqls() {
        return badSqls;
    }

    public static void main(String[] args) {
        EtlOperation etlOperation = new EtlOperation();
        try {
            etlOperation.parseSqlFile(PropertyFileUtil.getProperty("local_file_path.test_sql"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

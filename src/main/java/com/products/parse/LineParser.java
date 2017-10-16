package com.products.parse;

import java.util.*;
import java.util.Map.Entry;

import com.products.util.*;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import com.products.bean.Block;
import com.products.bean.ColLine;
import com.products.bean.QueryTree;
import com.products.bean.SQLResult;
import com.products.exception.SQLParseException;
import com.products.exception.UnSupportedException;


public class LineParser {

    private static final Boolean debugFlag = true;

    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";
    private static final String SPLIT_AND = "&";
    private static final String TOK_EOF = "<EOF>";
    private static final String CON_WHERE = "WHERE:";
    private static final String TOK_TMP_FILE = "TOK_TMP_FILE";

    private Map<String /*table*/, List<String/*column*/>> dbMap = new HashMap<String, List<String>>();
    private List<QueryTree> queryTreeList = new ArrayList<QueryTree>();

    private Stack<Set<String>> conditionsStack = new Stack<Set<String>>();
    private Stack<List<ColLine>> colsStack = new Stack<List<ColLine>>();

    private Map<String, List<ColLine>> resultQueryMap = new HashMap<String, List<ColLine>>();
    private Set<String> conditions = new HashSet<String>();
    private List<ColLine> cols = new ArrayList<ColLine>();

    private Stack<String> tableNameStack = new Stack<String>();
    private Stack<Boolean> joinStack = new Stack<Boolean>();
    private Stack<ASTNode> joinOnStack = new Stack<ASTNode>();

    private Map<String, QueryTree> queryMap = new HashMap<String, QueryTree>();
    private boolean joinClause = false;
    private ASTNode joinOn = null;
    private String nowQueryDB = "default";
    private boolean isCreateTable = false;


    private List<SQLResult> resultList = new ArrayList<SQLResult>();
    private List<ColLine> colLines = new ArrayList<ColLine>();
    private Set<String> outputTables = new HashSet<String>();
    private Set<String> inputTables = new HashSet<String>();
    private Set<String> dependenceColunmList = new HashSet<String>();


    private void parseIteral(ASTNode ast) {
        if(debugFlag) {
            System.out.printf("token name:" + ast.getText() + "\n");
        }
        prepareToParseCurrentNodeAndChilds(ast); //有join操作的时候入栈
        parseChildNodes(ast); //深度遍历，递归处理子节点
        parseCurrentNode(ast);
        endParseCurrentNode(ast);
    }


    private void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_CREATETABLE: // CREATE 入库表名
                    isCreateTable = true;
                    String tableOut = fillDB(BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0)));
                    outputTables.add(tableOut);
                    MetaCache.getInstance().init(tableOut);
                    break;
                case HiveParser.TOK_TAB: // INSERT 入库表名
                    String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    String tableOut2 = fillDB(tableTab);
                    outputTables.add(tableOut2);
                    MetaCache.getInstance().init(tableOut2);
                    break;
                case HiveParser.TOK_TABREF: //FROM 依赖表名
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableInFull = fillDB((tabTree.getChildCount() == 1) ? /*获取表名 <判断了是否带库名>*/
                            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            + SPLIT_DOT + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1))
                    );
                    String tableIn = tableInFull.substring(tableInFull.indexOf(SPLIT_DOT) + 1);
                    inputTables.add(tableInFull);
                    MetaCache.getInstance().init(tableInFull);
                    queryMap.clear();
                    String alia = null;
                    if (ast.getChild(1) != null) { //判断是否有表的别名
                        alia = ast.getChild(1).getText().toLowerCase();
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(alia);
                        qt.getTableSet().add(tableInFull);
                        QueryTree pTree = getSubQueryParent(ast); /*没看懂*/
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt); // queryTreeList 干嘛用的
                        if (joinClause && ast.getParent() == joinOn) {
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry); //queryMap 干嘛用的
                                }
                            }
                        } else {
                            queryMap.put(qt.getCurrent(), qt);
                        }
                    } else { //没有别名用表名 作为current
                        alia = tableIn.toLowerCase();
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(alia);
                        qt.getTableSet().add(tableInFull);
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt);

                        if (joinClause && ast.getParent() == joinOn) {
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {//为什么要put两次? 一次带库名+表名  还一次表名
                            queryMap.put(qt.getCurrent(), qt);
                            queryMap.put(tableInFull.toLowerCase(), qt);
                        }
                    }
                    break;
                case HiveParser.TOK_SUBQUERY:
                    if (ast.getChildCount() == 2) { //通过判断是否有别名, 说明子阶段已经完成, 需处理queryTreeList
                        String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
                        String aliaReal = "";
                        if (aliaReal.length() != 0) {
                            aliaReal = aliaReal.substring(0, aliaReal.length() - 1);
                        }

                        QueryTree qt = new QueryTree();
                        qt.setCurrent(tableAlias.toLowerCase());
                        qt.setColLineList(generateColLineList(cols, conditions));
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setId(generateTreeId(ast));
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        qt.setChildList(getSubQueryChilds(qt.getId()));
                        if (Check.notEmpty(qt.getChildList())) {
                            for (QueryTree cqt : qt.getChildList()) {
                                qt.getTableSet().addAll(cqt.getTableSet());
                                queryTreeList.remove(cqt);
                            }
                        }
                        queryTreeList.add(qt);
                        cols.clear();

                        queryMap.clear();
                        for (QueryTree _qt : queryTreeList) {
                            if (qt.getParent().equals(_qt.getParent())) {
                                queryMap.put(_qt.getCurrent(), _qt);
                            }
                        }
                    }
                    break;
                case HiveParser.TOK_SELEXPR: // SELECT 字段
                    Tree parentParentTree = ast.getParent().getParent();
                    if (parentParentTree.getText().equals("TOK_LATERAL_VIEW")) {
                        /*
                        * lateral view = left join (只是没有on)
                        * select 字段  from上面那张表
                        * TODO
                        * */

                    }
                    else {
                        Tree child = parentParentTree.getChild(0).getChild(0);
                        String tName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) child.getChild(0));
                        String destTable = TOK_TMP_FILE.equals(tName) ? TOK_TMP_FILE : fillDB(tName); // 获取将要插入的表名

                        if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                            String tableOrAlias = "";
                            if (ast.getChild(0).getChild(0) != null) {
                                tableOrAlias = ast.getChild(0).getChild(0).getChild(0).getText();
                            }
                            String[] result = getTableAndAlia(tableOrAlias);
                            String _alia = result[1];

                            boolean isSub = false; //处理嵌套select*的情况
                            if (Check.notEmpty(_alia)) {
                                for (String string : _alia.split(SPLIT_AND)) {
                                    QueryTree qt = queryMap.get(string.toLowerCase());
                                    if (null != qt) {
                                        List<ColLine> colLineList = qt.getColLineList();
                                        if (Check.notEmpty(colLineList)) {
                                            isSub = true;
                                            for (ColLine colLine : colLineList) {
                                                cols.add(colLine);
                                            }
                                        }
                                    }
                                }
                            }
                            if (!isSub) {
                                String nowTable = result[0];
                                String[] tableArr = nowTable.split(SPLIT_AND);
                                for (String tables : tableArr) {
                                    String[] split = tables.split("\\.");
                                    if (split.length > 2) {
                                        throw new SQLParseException("parse table:" + nowTable);
                                    }
                                    List<String> colByTab = MetaCache.getInstance().getColumnByDBAndTable(tables);
                                    for (String column : colByTab) {
                                        Set<String> fromNameSet = new LinkedHashSet<String>();
                                        fromNameSet.add(tables + SPLIT_DOT + column);
                                        ColLine cl = new ColLine(column, tables + SPLIT_DOT + column, fromNameSet,
                                                new LinkedHashSet<String>(), destTable, column);
                                        //XIWU
                                        for (String dependenceColunms : cl.getFromNameSet()) {
                                            //System.out.print("dependenceColunms: " + dependenceColunms + "\n");
                                            for (String dependenceColunm : dependenceColunms.split("&")) {
                                                if (dependenceColunm.split("\\.").length == 3) {
                                                    dependenceColunmList.add(dependenceColunm);
                                                    break;
                                                }
                                            }
                                        }
                                        System.out.print("1" + JsonUtil.objectToJson(cl) + "\n");
                                        cols.add(cl);
                                    }
                                }
                            }
                        } else {
                            Block bk = getBlockIteral((ASTNode) ast.getChild(0));
                            String toNameParse = getToNameParse(ast, bk);
                            Set<String> fromNameSet = filterData(bk.getColSet());
                            ColLine cl = new ColLine(toNameParse, bk.getCondition(), fromNameSet, new LinkedHashSet<String>(), destTable, "");
                            //XIWU
                            for (String dependenceColunms : cl.getFromNameSet()) {
                                //System.out.print("dependenceColunms: " + dependenceColunms + "\n");
                                for (String dependenceColunm : dependenceColunms.split("&")) {
                                    if (dependenceColunm.split("\\.").length == 3) {
                                        dependenceColunmList.add(dependenceColunm);
                                    }
                                }
                            }
                            System.out.print("2"+ JsonUtil.objectToJson(cl) + "\n");
                            cols.add(cl);
                        }
                    }
                    break;
                case HiveParser.TOK_WHERE: // WHERE 条件
                    // xiwu
                    for (String dependenceColunm: getBlockIteral((ASTNode) ast.getChild(0)).getColSet()) {
                        if (!dependenceColunm.contains("$") && dependenceColunm.split("\\.").length == 3) {
                            dependenceColunmList.add(dependenceColunm.replace("'", ""));
                        }
                    }
                    conditions.add(CON_WHERE + getBlockIteral((ASTNode) ast.getChild(0)).getCondition());
                    break;
                default:
                    // 按照栈的后进先出的顺序，依次处理Join操作
                    if (joinOn != null && joinOn.getTokenStartIndex() == ast.getTokenStartIndex()
                            && joinOn.getTokenStopIndex() == ast.getTokenStopIndex()) {
                        ASTNode astCon = (ASTNode) ast.getChild(2);

                        // xiwu

                        for(String columnName: getBlockIteral(astCon).getColSet()) {
                            if (!columnName.contains("$") && columnName.split("\\.").length == 3) {
                                dependenceColunmList.add(columnName.replace("'", ""));
                            }
                        }


                        conditions.add(ast.getText().substring(4) + ":" + getBlockIteral(astCon).getCondition());
                        break;
                    }
            }
        }
    }


    private QueryTree getSubQueryParent(Tree ast) {
        Tree _tree = ast;
        QueryTree qt = new QueryTree();
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_SUBQUERY) {
                qt.setpId(generateTreeId(_tree));
                qt.setParent(BaseSemanticAnalyzer.getUnescapedName((ASTNode) _tree.getChild(1)));
                return qt;
            }
        }
        qt.setpId(-1);
        qt.setParent("NIL");
        return qt;
    }

    private int generateTreeId(Tree tree) {
        return tree.getTokenStartIndex() + tree.getTokenStopIndex();
    }


    private List<QueryTree> getSubQueryChilds(int id) {
        List<QueryTree> list = new ArrayList<QueryTree>();
        for (int i = 0; i < queryTreeList.size(); i++) {
            QueryTree qt = queryTreeList.get(i);
            if (id == qt.getpId()) {
                list.add(qt);
            }
        }
        return list;
    }

    private String getToNameParse(ASTNode ast, Block bk) {
        String alia = "";
        Tree child = ast.getChild(0);
        if (ast.getChild(1) != null) {
            alia = ast.getChild(1).getText();
        } else if (child.getType() == HiveParser.DOT
                && child.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && child.getChild(0).getChildCount() == 1
                && child.getChild(1).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(1).getText());
        } else if (child.getType() == HiveParser.TOK_TABLE_OR_COL
                && child.getChildCount() == 1
                && child.getChild(0).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
        }
        return alia;
    }


    private Block getBlockIteral(ASTNode ast) {
        try {
            if (ast.getType() == HiveParser.KW_OR
                    || ast.getType() == HiveParser.KW_AND) {
                Block bk1 = getBlockIteral((ASTNode) ast.getChild(0));
                Block bk2 = getBlockIteral((ASTNode) ast.getChild(1));
                bk1.getColSet().addAll(bk2.getColSet());
                bk1.setCondition("(" + bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition() + ")");
                return bk1;
            } else if (ast.getType() == HiveParser.NOTEQUAL
                    || ast.getType() == HiveParser.EQUAL
                    || ast.getType() == HiveParser.LESSTHAN
                    || ast.getType() == HiveParser.LESSTHANOREQUALTO
                    || ast.getType() == HiveParser.GREATERTHAN
                    || ast.getType() == HiveParser.GREATERTHANOREQUALTO
                    || ast.getType() == HiveParser.KW_LIKE
                    || ast.getType() == HiveParser.DIVIDE
                    || ast.getType() == HiveParser.PLUS
                    || ast.getType() == HiveParser.MINUS
                    || ast.getType() == HiveParser.STAR
                    || ast.getType() == HiveParser.MOD
                    || ast.getType() == HiveParser.AMPERSAND
                    || ast.getType() == HiveParser.TILDE
                    || ast.getType() == HiveParser.BITWISEOR
                    || ast.getType() == HiveParser.BITWISEXOR) {
                Block bk1 = getBlockIteral((ASTNode) ast.getChild(0));
                if (ast.getChild(1) == null) { // -1
                    bk1.setCondition(ast.getText() + bk1.getCondition());
                } else {
                    Block bk2 = getBlockIteral((ASTNode) ast.getChild(1));
                    bk1.getColSet().addAll(bk2.getColSet());
                    bk1.setCondition(bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition());
                }
                return bk1;
            } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) {
                Block col = getBlockIteral((ASTNode) ast.getChild(1));
                String condition = ast.getChild(0).getText();
                col.setCondition(condition + "(distinct (" + col.getCondition() + "))");
                return col;
            } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
                String fun = ast.getChild(0).getText();
                Block col = ast.getChild(1) == null ? new Block() : getBlockIteral((ASTNode) ast.getChild(1));
                if ("when".equalsIgnoreCase(fun)) {
                    col.setCondition(getWhenCondition(ast));
                    Set<Block> processChilds = processChilds(ast, 1);
                    col.getColSet().addAll(bkToCols(col, processChilds));
                    return col;
                } else if ("IN".equalsIgnoreCase(fun)) {
                    col.setCondition(col.getCondition() + " in (" + blockCondToString(processChilds(ast, 2)) + ")");
                    return col;
                } else if ("TOK_ISNOTNULL".equalsIgnoreCase(fun)
                        || "TOK_ISNULL".equalsIgnoreCase(fun)) {
                    col.setCondition(col.getCondition() + " " + fun.toLowerCase().substring(4));
                    return col;
                } else if ("BETWEEN".equalsIgnoreCase(fun)) {
                    col.setCondition(getBlockIteral((ASTNode) ast.getChild(2)).getCondition()
                            + " between " + getBlockIteral((ASTNode) ast.getChild(3)).getCondition()
                            + " and " + getBlockIteral((ASTNode) ast.getChild(4)).getCondition());
                    return col;
                }
                Set<Block> processChilds = processChilds(ast, 1);
                col.getColSet().addAll(bkToCols(col, processChilds));
                col.setCondition(fun + "(" + blockCondToString(processChilds) + ")");
                return col;
            } else if (ast.getType() == HiveParser.LSQUARE) {
                Block column = getBlockIteral((ASTNode) ast.getChild(0));
                Block key = getBlockIteral((ASTNode) ast.getChild(1));
                column.setCondition(column.getCondition() + "[" + key.getCondition() + "]");
                return column;
            } else {
                return parseBlock(ast);
            }
        } catch (Exception e) {
            System.out.print("空指针" + e + "\n");
            return new Block();
        }
    }


    private Set<String> bkToCols(Block col, Set<Block> processChilds) {
        Set<String> set = new LinkedHashSet<String>(processChilds.size());
        for (Block colLine : processChilds) {
            if (Check.notEmpty(colLine.getColSet())) {
                set.addAll(colLine.getColSet());
            }
        }
        return set;
    }

    private String blockCondToString(Set<Block> processChilds) {
        StringBuilder sb = new StringBuilder();
        for (Block colLine : processChilds) {
            sb.append(colLine.getCondition()).append(SPLIT_COMMA);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }


    private String getWhenCondition(ASTNode ast) {
        int cnt = ast.getChildCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < cnt; i++) {
            String condition = getBlockIteral((ASTNode) ast.getChild(i)).getCondition();
            if (i == 1) {
                sb.append("(case when " + condition);
            } else if (i == cnt - 1) {
                sb.append(" else " + condition + " end)");
            } else if (i % 2 == 0) {
                sb.append(" then " + condition);
            } else {
                sb.append(" when " + condition);
            }
        }
        return sb.toString();
    }


    private void putResultQueryMap(int sqlIndex, String tableAlias) {
        List<ColLine> list = generateColLineList(cols, conditions);
        String key = sqlIndex == 0 ? tableAlias : tableAlias + sqlIndex;
        resultQueryMap.put(key, list);
    }

    private List<ColLine> generateColLineList(List<ColLine> cols, Set<String> conditions) {
        List<ColLine> list = new ArrayList<ColLine>();
        for (ColLine entry : cols) {
            entry.getConditionSet().addAll(conditions);
            list.add(ParseUtil.cloneColLine(entry));
        }
        return list;
    }


    private boolean notNormalCol(String column) {
        return Check.isEmpty(column) || NumberUtil.isNumeric(column)
                || (column.startsWith("\"") && column.endsWith("\""))
                || (column.startsWith("\'") && column.endsWith("\'"));
    }


    private Set<Block> processChilds(ASTNode ast, int startIndex) {
        int cnt = ast.getChildCount();
        Set<Block> set = new LinkedHashSet<Block>();
        for (int i = startIndex; i < cnt; i++) {
            Block bk = getBlockIteral((ASTNode) ast.getChild(i));
            if (Check.notEmpty(bk.getCondition()) || Check.notEmpty(bk.getColSet())) {
                set.add(bk);
            }
        }
        return set;
    }


    private Block parseBlock(ASTNode ast) {
        if (ast.getType() == HiveParser.DOT
                && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1
                && ast.getChild(1).getType() == HiveParser.Identifier) {

            String column = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
            String alia = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getChild(0).getText());

            
            return getBlock(column, alia, new HashMap());
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            String column = ast.getChild(0).getText();


            // xiwu 解决id来源多表问题
            HashMap<String, List> dependenceTableColumn = new HashMap<String, List>();
            for (String dependenceColumn: dependenceColunmList) {
                String dependenceTable = dependenceColumn.split("\\.")[0] + "." + dependenceColumn.split("\\.")[1];
                if (dependenceTableColumn.containsKey(dependenceTable)) {
                    dependenceTableColumn.get(dependenceTable).add(dependenceColumn.split("\\.")[2]);
                } else {
                    List columnList = new LinkedList<String>();
                    columnList.add(dependenceColumn.split("\\.")[2]);
                    dependenceTableColumn.put(dependenceTable, columnList);
                }
            }


            return getBlock(column, null, dependenceTableColumn);
        } else if (ast.getType() == HiveParser.Number
                || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            Block bk = new Block();
            bk.setCondition(ast.getText());
            bk.getColSet().add(ast.getText());
            return bk;
        }
        return new Block();
    }


    private Block getBlock(String column, String alia, HashMap<String, List> tableColumn) {
        String[] result = getTableAndAlia(alia);
        String tableArray = result[0];
        String _alia = result[1];
        for (String string : _alia.split(SPLIT_AND)) {
            QueryTree qt = queryMap.get(string.toLowerCase());

            if (Check.notEmpty(column)) {
                // xiwu
                if (qt != null ) {
                    for (ColLine colLine : qt.getColLineList()) {
                        if (column.equalsIgnoreCase(colLine.getToNameParse())) {
                            Block bk = new Block();
                            bk.setCondition(colLine.getColCondition());
                            bk.setColSet(ParseUtil.cloneSet(colLine.getFromNameSet()));
                            return bk;
                        }
                    }
                }
            }
        }

        String _realTable = tableArray;
        int cnt = 0;
        for (String tables : tableArray.split(SPLIT_AND)) {

            // xiwu
            if (tableColumn.containsKey(tables)) {  // 从已经存在的table里找
                if (tableColumn.get(tables).contains(column)) {
                    _realTable = tables;
                    cnt++;
                } else {
                    String[] split = tables.split("\\.");
                    if (split.length > 2) {
                        throw new SQLParseException("parse table:" + tables);
                    }
                    List<String> colByTab = MetaCache.getInstance().getColumnByDBAndTable(tables);
                    for (String col : colByTab) {
                        if (column.equalsIgnoreCase(col)) {
                            _realTable = tables;
                        }
                    }
                }
            } else {

                String[] split = tables.split("\\.");
                if (split.length > 2) {
                    throw new SQLParseException("parse table:" + tables);
                }
                List<String> colByTab = MetaCache.getInstance().getColumnByDBAndTable(tables);
                for (String col : colByTab) {
                    if (column.equalsIgnoreCase(col)) {
                        _realTable = tables;
                        cnt++;
                    }
                }
            }
        }

        if (cnt > 1) {
            throw new SQLParseException("SQL is ambiguity, column: " + column + " tables:" + tableArray);
        }

        Block bk = new Block();
        bk.setCondition(_realTable + SPLIT_DOT + column);
        bk.getColSet().add(_realTable + SPLIT_DOT + column);
        return bk;
    }

    private Set<String> filterData(Set<String> colSet) {
        Set<String> set = new LinkedHashSet<String>();
        for (String string : colSet) {
            if (!notNormalCol(string)) {
                set.add(string);
            }
        }
        return set;
    }


    private void parseChildNodes(ASTNode ast) {
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                parseIteral(child);
            }
        }
    }

    private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_SWITCHDATABASE:
                    System.out.println("nowQueryDB changed " + nowQueryDB + " to " + ast.getChild(0).getText());
                    nowQueryDB = ast.getChild(0).getText();
                    break;
                case HiveParser.TOK_TRANSFORM:
                    throw new UnSupportedException("no support transform using clause");
                // Q: Join这是神马操作，为啥要入栈
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                //case HiveParser.TOK_LATERAL_VIEW: //看看laterview怎么弄
                case HiveParser.TOK_UNIQUEJOIN:
                    joinStack.push(joinClause);
                    joinClause = true;
                    joinOnStack.push(joinOn);
                    joinOn = ast;
                    break;
            }
        }
    }

    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            Tree parent = ast.getParent();
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN: //处理完Join操作后，进行出栈操作
                    joinClause = joinStack.pop();
                    joinOn = joinOnStack.pop();
                    break;

                case HiveParser.TOK_QUERY:
                    processUnionStack(ast, parent);
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_SELECT:
                    break;
                case HiveParser.TOK_UNIONALL:
                    mergeUnionCols();
                    processUnionStack(ast, parent);
                    break;
            }
        }
    }

    private void mergeUnionCols() {
        validateUnion(cols);
        int size = cols.size();
        int colNum = size / 2;
        List<ColLine> list = new ArrayList<ColLine>(colNum);
        for (int i = 0; i < colNum; i++) {
            ColLine col = cols.get(i);
            for (int j = i + colNum; j < size; j = j + colNum) {
                ColLine col2 = cols.get(j);
                list.add(col2);
                if (notNormalCol(col.getToNameParse()) && !notNormalCol(col2.getToNameParse())) {
                    col.setToNameParse(col2.getToNameParse());
                }
                col.getFromNameSet().addAll(col2.getFromNameSet());

                col.setColCondition(col.getColCondition() + SPLIT_AND + col2.getColCondition());

                Set<String> conditionSet = ParseUtil.cloneSet(col.getConditionSet());
                conditionSet.addAll(col2.getConditionSet());
                conditionSet.addAll(conditions);
                col.getConditionSet().addAll(conditionSet);
            }
        }
        cols.removeAll(list);
    }

    private void processUnionStack(ASTNode ast, Tree parent) {
        boolean isNeedAdd = parent.getType() == HiveParser.TOK_UNIONALL;
        if (isNeedAdd) {
            if (parent.getChild(0) == ast && parent.getChild(1) != null) {
                conditionsStack.push(ParseUtil.cloneSet(conditions));
                conditions.clear();
                colsStack.push(ParseUtil.cloneList(cols));
                cols.clear();
            } else {

                if (!conditionsStack.isEmpty()) {
                    conditions.addAll(conditionsStack.pop());
                }
                if (!colsStack.isEmpty()) {
                    cols.addAll(0, colsStack.pop());
                }
            }
        }
    }

    private void parseAST(ASTNode ast) {
        parseIteral(ast);
    }

    public List<SQLResult> parse(String sqlAll) throws Exception {
        if (Check.isEmpty(sqlAll)) {
            return resultList;
        }
        startParseAll();
        int i = 0;
        for (String sql : sqlAll.split("(?<!\\\\);")) {
            ParseDriver pd = new ParseDriver();
            String trim = sql.toLowerCase().trim();
            if (trim.startsWith("set") || trim.startsWith("add") || Check.isEmpty(trim)) {
                continue;
            }
            HiveConf hiveConf = new HiveConf() ;
            Configuration conf = new Configuration(hiveConf);
            conf.set("_hive.hdfs.session.path","/tmp");
            conf.set("_hive.local.session.path","/tmp");
            Context context = new Context(conf);

            ASTNode ast = pd.parse(sql,context);
            if(debugFlag) {

                System.out.println(ast.dump()); //树结构
                // if ("local".equals(PropertyFileUtil.getProperty("environment"))) {
                //}
            }
            prepareParse();
            parseAST(ast);
            endParse(++i);
        }
        return resultList;
    }

    private void startParseAll() {
        resultList.clear();
    }

    private void prepareParse() {
        isCreateTable = false;
        dbMap.clear();

        colLines.clear();
        outputTables.clear();
        inputTables.clear();

        queryMap.clear();
        queryTreeList.clear();

        conditionsStack.clear();
        colsStack.clear();

        resultQueryMap.clear();
        conditions.clear();
        cols.clear();

        tableNameStack.clear();
        joinStack.clear();
        joinOnStack.clear();

        joinClause = false;
        joinOn = null;
    }


    private void endParse(int sqlIndex) {
        putResultQueryMap(sqlIndex, TOK_EOF);
        putDBMap();
        setColLineList();
    }


    private void setColLineList() {
        Map<String, List<ColLine>> map = new HashMap<String, List<ColLine>>();
        for (Entry<String, List<ColLine>> entry : resultQueryMap.entrySet()) {
            if (entry.getKey().startsWith(TOK_EOF)) {
                List<ColLine> value = entry.getValue();
                for (ColLine colLine : value) {
                    List<ColLine> list = map.get(colLine.getToTable());
                    if (Check.isEmpty(list)) {
                        list = new ArrayList<ColLine>();
                        map.put(colLine.getToTable(), list);
                    }
                    list.add(colLine);
                }
            }
        }

        for (Entry<String, List<ColLine>> entry : map.entrySet()) {
            String table = entry.getKey();
            List<ColLine> pList = entry.getValue();
            List<String> dList = dbMap.get(table);
            int metaSize = Check.isEmpty(dList) ? 0 : dList.size();
            for (int i = 0; i < pList.size(); i++) {
                ColLine clp = pList.get(i);
                String colName = null;
                if (i < metaSize) {
                    colName = table + SPLIT_DOT + dList.get(i);
                }
                if (isCreateTable && TOK_TMP_FILE.equals(table)) {
                    for (String string : outputTables) {
                        table = string;
                    }
                }
                ColLine colLine = new ColLine(clp.getToNameParse(), clp.getColCondition(),
                        clp.getFromNameSet(), clp.getConditionSet(), table, colName); // 一个字段对应的解析
                colLines.add(colLine);
            }
        }

        if (Check.notEmpty(colLines)) {
            SQLResult sr = new SQLResult();
            sr.setColLineList(ParseUtil.cloneList(colLines));
            sr.setInputTables(ParseUtil.cloneSet(inputTables));
            sr.setOutputTables(ParseUtil.cloneSet(outputTables));
            sr.setDependenceColunmList(ParseUtil.cloneSet(dependenceColunmList));
            resultList.add(sr);
        }
    }


    private void putDBMap() {
        for (String table : outputTables) {
            List<String> list = MetaCache.getInstance().getColumnByDBAndTable(table);
            dbMap.put(table, list);
        }
    }


    private String fillDB(String nowTable) {
        if (Check.isEmpty(nowTable)) {
            return nowTable;
        }
        StringBuilder sb = new StringBuilder();
        String[] tableArr = nowTable.split(SPLIT_AND);
        for (String tables : tableArr) {
            String[] split = tables.split("\\" + SPLIT_DOT);
            if (split.length > 2) {
                System.out.println(tables);
                throw new SQLParseException("parse table:" + nowTable);
            }
            String db = split.length == 2 ? split[0] : nowQueryDB;
            String table = split.length == 2 ? split[1] : split[0];
            sb.append(db).append(SPLIT_DOT).append(table).append(SPLIT_AND);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }


    private String[] getTableAndAlia(String alia) {
        String _alia = Check.notEmpty(alia) ? alia :
                ParseUtil.collectionToString(queryMap.keySet(), SPLIT_AND, true);
        String[] result = {"", _alia};
        Set<String> tableSet = new HashSet<String>();
        if (Check.notEmpty(_alia)) {
            String[] split = _alia.split(SPLIT_AND);
            for (String string : split) {
                if (inputTables.contains(string) || inputTables.contains(fillDB(string))) {
                    tableSet.add(fillDB(string));
                } else if (queryMap.containsKey(string.toLowerCase())) {
                    tableSet.addAll(queryMap.get(string.toLowerCase()).getTableSet());
                }
            }
            result[0] = ParseUtil.collectionToString(tableSet, SPLIT_AND, true);
            result[1] = _alia;
        }
        return result;
    }

    private void validateUnion(List<ColLine> list) {
        int size = list.size();
        if (size % 2 == 1) {
            throw new SQLParseException("union column number are different, size=" + size);
        }
        int colNum = size / 2;
        checkUnion(list, 0, colNum);
        checkUnion(list, colNum, size);
    }

    private void checkUnion(List<ColLine> list, int start, int end) {
        String tmp = null;
        for (int i = start; i < end; i++) {
            ColLine col = list.get(i);
            if (Check.isEmpty(tmp)) {
                tmp = col.getToTable();
            } else if (!tmp.equals(col.getToTable())) {
                throw new SQLParseException("union column number/types are different,table1=" + tmp + ",table2=" + col.getToTable());
            }
        }
    }

}

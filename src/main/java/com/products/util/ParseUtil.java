package com.products.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.products.bean.ColLine;
import com.products.bean.DWTask;
import com.products.exception.SQLExtractException;

public final class ParseUtil {
    private static final Map<Integer, String> hardcodeScriptMap = new HashMap<Integer, String>();
    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";

    private static final String REGEX_HIVE = "^\\s*(\\$exe_)?hive\\d?.*?-e\\s*\"([\\s\\S]*?)\"";
    private static final String REGEX_SOURCE = "^\\s*source\\s*([\\S]*)\\s*";

    private static final String REGEX_SHELL = "^\\s*sh\\s*(-\\S)?\\s*([\\S]*)\\s*";
    private static final String REGEX_ALONE_VAR_VALUE = "\\s*=\\s*[\"]([\\s\\S]*?)[\"]";

    private static final String REGEX_VAR = "[\\s\\(]+(\\$\\{?[\\w]*\\}?)[\\s\\)]+";

    private static final String REGEX_PYTHON_SQL_VAR = "runHiveCmd\\((\\w*?)\\s*,";
    private static final String REGEX_PYTHON_SQL_VAR_VALUE = "\\s*=\\s*\"\"\"([\\s\\S]*?)\"\"\"";
    private static final String REGEX_PYTHON_SQL_INVAR = "\\s+%\\(([\\s\\S]*?)\\)s[;\\s]";
    private static final String REGEX_PYTHON_SQL_INVAR_VALUE = "\\s*=\\s*[\"\']([\\s\\S]*?)[\"\']";

    private static final Map<String, Boolean> REGEX_MULTI_VAR_VALUE = new HashMap<String, Boolean>();

    static {
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*\"([\\s\\S]*?)\"", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*\'([\\s\\S]*?)\'", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*(\\w*)", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*`([\\s\\S]*?)`", true);

        hardcodeScriptMap.put(400, "^\\s*hive\\d?.*?-e\\s*\"([\\s\\S]*)\"");
    }

    public static String[] parseDBTable(String table) {
        return table.split("\\" + SPLIT_DOT);
    }

    public static String collectionToString(Collection<String> coll) {
        return collectionToString(coll, SPLIT_COMMA, true);
    }

    public static String collectionToString(Collection<String> coll, String split, boolean isCheck) {
        StringBuilder sb = new StringBuilder();
        if (Check.notEmpty(coll)) {
            for (String string : coll) {
                if ((isCheck && Check.notEmpty(string)) || !isCheck) {
                    sb.append(string).append(split);
                }
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - split.length());
            }
        }
        return sb.toString();
    }

    public static String uniqMerge(String s1, String s2) {
        Set<String> set = new HashSet<String>();
        set.add(s1);
        set.add(s2);
        return collectionToString(set);
    }

    public static List<String> extractSQL(DWTask task) {
        String path = task.getPath();
        if (path.endsWith("sh")) {
            String content = fixSourceVar(path, read(path));
            List<String> _sqlList = regexList(REGEX_HIVE, content, 2);
            if (Check.notEmpty(_sqlList)) {
                return fixSqlVarList(content, _sqlList);
            }

            List<String> resulList = new ArrayList<String>();
            List<String> scriptList = regexList(REGEX_SHELL, content, 2);
            if (Check.notEmpty(scriptList)) {
                for (String script : scriptList) {
                    if (!script.contains("#") && script.contains(".")) {
                        String newPath = getFilePath(path, script);
                        String newPathContent = fixSourceVar(newPath, read(newPath));
                        String rsql = getSQL(task.getId(), newPathContent);
                        if (Check.notEmpty(rsql)) {
                            _sqlList.add(rsql);
                        }
                        if (Check.notEmpty(_sqlList)) {
                            resulList.addAll(fixSqlVarList(newPathContent, _sqlList));
                        }
                    }
                }
            }
            return resulList;
        } else if (path.endsWith("py")) {
            String content = read(path);
            List<String> sqlVarList = regexList(REGEX_PYTHON_SQL_VAR, content, 1);
            List<String> resulList = new ArrayList<String>();
            for (String sqlVar : sqlVarList) {
                final String sql = regex(sqlVar + REGEX_PYTHON_SQL_VAR_VALUE, content, 1);
                String _sql = sql;
                if (Check.notEmpty(sql) && sql.toLowerCase().contains("insert") && sql.toLowerCase().contains("table")) {
                    List<String> sqlInVarList = regexList(REGEX_PYTHON_SQL_INVAR, sql, 1);
                    for (String sqlInVar : sqlInVarList) {
                        String value = regex(sqlInVar + REGEX_PYTHON_SQL_INVAR_VALUE, content, 1);
                        if (Check.isEmpty(value)) {
                            throw new SQLExtractException("can extract var:" + sqlInVar);
                        }
                        _sql = _sql.replaceAll(escape("%(" + sqlInVar + ")s"), Matcher.quoteReplacement(value));
                    }
                    resulList.add(_sql);
                }
            }
            return resulList;
        }
        throw new SQLExtractException("can extract sql");
    }

    private static String fixSourceVar(String path, String content) {
        List<String> sourceList = regexList(REGEX_SOURCE, content, 1);
        StringBuilder sb = new StringBuilder();
        if (Check.notEmpty(sourceList)) {
            for (String source : sourceList) {
                if (source.contains(".")) {
                    String newPath = getFilePath(path, source);
                    String newPathContent = read(newPath);
                    sb.append(newPathContent);
                }
            }
        }
        sb.append(content);
        return sb.toString();
    }


    private static String getFilePath(String path, String source) {
        int lastIndexOf = source.lastIndexOf("/");
        String name = lastIndexOf > -1 ? source.substring(lastIndexOf + 1) : source;
        String newPath = path.substring(0, path.lastIndexOf("/") + 1) + name;
        return newPath;
    }

    private static String getSQL(long taskId, String newPathContent) {
        String rsql;
        if (hardcodeScriptMap.containsKey(taskId)) {
            rsql = regex(hardcodeScriptMap.get(taskId), newPathContent, 1);
        } else {
            rsql = regex(REGEX_HIVE, newPathContent, 2);
        }
        return rsql;
    }


    private static List<String> fixSqlVarList(String content, List<String> sqlList) {
        List<String> list = new ArrayList<String>(sqlList.size());
        for (String tsql : sqlList) {
            list.add(fixSqlVar(content, tsql));
        }
        return list;
    }

    private static String fixSqlVar(String content, String tsql) {
        String _tsql = tsql;
        boolean isVarAlone = _tsql.trim().startsWith("$");
        Set<String> varSet = matchShellVar(tsql, isVarAlone);
        if (Check.notEmpty(varSet)) {
            for (String var : varSet) {
                String _var;
                if (var.startsWith("${") && var.endsWith("}")) {
                    _var = var.substring(2, var.length() - 1);
                } else {
                    _var = var.substring(1);
                }
                if (isVarAlone) {
                    _tsql = regex(_var + REGEX_ALONE_VAR_VALUE, content, 1);
                    if (Check.notEmpty(matchShellVar(_tsql, false))) {
                        _tsql = fixSqlVar(content, _tsql);
                    }
                } else {
                    String value = getVarValue(content, _var);
                    if (Check.isEmpty(value)) {
                        throw new SQLExtractException("can extract var:" + _var);
                    }
                    _tsql = _tsql.replaceAll(escape(var), Matcher.quoteReplacement(value));
                }
            }
        }
        return _tsql;
    }

    public static String escape(String keyword) {
        if (Check.notEmpty(keyword)) {
            String[] fbsArr = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }

    private static String getVarValue(String content, String _var) {
        String value = null;
        for (Entry<String, Boolean> entry : REGEX_MULTI_VAR_VALUE.entrySet()) {
            String res = regex(_var + entry.getKey(), content, 1);
            if (Check.notEmpty(res)) {
                return entry.getValue() ? "(" + res + ")" : res;
            }
        }
        return value;
    }


    private static Set<String> matchShellVar(String sql, boolean varAlone) {
        Set<String> set = new HashSet<String>();
        if (varAlone) {
            set.add(sql);
        } else {
            set.addAll(regexList(REGEX_VAR, sql, 1));
        }
        return set;
    }

    private static String read(String path) {
        String property = PropertyFileUtil.getProperty("file.source");
        if ("local".equals(property)) {
            return FileUtil.read(path);
        } else if ("hdfs".equals(property)) {
            return HDPFileUtil.read4Linux(path);
        }
        return "";
    }


    private static String regex(String regex, String content, int group) {
        List<String> regexList = regexList(regex, content, group);
        return Check.isEmpty(regexList) ? "" : regexList.get(0);
    }

    private static List<String> regexList(String regex, String content, int group) {
        Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(content);
        List<String> list = new ArrayList<String>();
        while (matcher.find()) {
            list.add(matcher.group(group));
        }
        return list;
    }

    public static Map<String, String> cloneAliaMap(Map<String, String> map) {
        Map<String, String> map2 = new HashMap<String, String>(map.size());
        for (Entry<String, String> entry : map.entrySet()) {
            map2.put(entry.getKey(), entry.getValue());
        }
        return map2;
    }

    public static Map<String, List<ColLine>> cloneSubQueryMap(Map<String, List<ColLine>> map) {
        Map<String, List<ColLine>> map2 = new HashMap<String, List<ColLine>>(map.size());
        for (Entry<String, List<ColLine>> entry : map.entrySet()) {
            List<ColLine> value = entry.getValue();
            List<ColLine> list = new ArrayList<ColLine>(value.size());
            for (ColLine colLine : value) {
                list.add(cloneColLine(colLine));
            }
            map2.put(entry.getKey(), value);
        }
        return map2;
    }


    public static ColLine cloneColLine(ColLine col) {
        return new ColLine(col.getToNameParse(), col.getColCondition(),
                cloneSet(col.getFromNameSet()), cloneSet(col.getConditionSet()),
                col.getToTable(), col.getToName());
    }


    public static Set<String> cloneSet(Set<String> set) {
        Set<String> set2 = new HashSet<String>(set.size());
        for (String string : set) {
            set2.add(string);
        }
        return set2;
    }

    public static List<ColLine> cloneList(List<ColLine> list) {
        List<ColLine> list2 = new ArrayList<ColLine>(list.size());
        for (ColLine col : list) {
            list2.add(cloneColLine(col));
        }
        return list2;
    }


    private ParseUtil() {
    }
}

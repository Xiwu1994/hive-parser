package com.products.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.products.bean.ColLine;
import com.products.bean.SQLResult;
import com.products.exception.VolidateException;
import com.products.util.Check;
import com.products.util.MetaCache;
import com.products.util.NumberUtil;

public class Validater {

    private Map<String/*table*/, List<String/*column*/>> dbMap = new HashMap<String, List<String>>();

    public void validate(List<SQLResult> srList) {

        for (SQLResult sr : srList) {
            Set<String> inputTables = sr.getInputTables();
            Set<String> outputTables = sr.getOutputTables();
            List<ColLine> colLineList = sr.getColLineList();

            if (Check.isEmpty(outputTables)) {
                throw new VolidateException("no output table");
            }
            if (Check.isEmpty(inputTables)) {
                throw new VolidateException("no input table");
            }

            for (String table : inputTables) {
                if (!MetaCache.getInstance().getTableMap().containsKey(table.toLowerCase())) {
                    throw new VolidateException("input table not exist: " + table.toLowerCase());
                }
            }

            for (String table : outputTables) {
                if (!MetaCache.getInstance().getTableMap().containsKey(table.toLowerCase())) {
                    throw new VolidateException("out table not exist: " + table.toLowerCase());
                }
            }


            Map<String, List<ColLine>> map = new HashMap<String, List<ColLine>>();
            for (ColLine colLine : colLineList) {
                List<ColLine> list = map.get(colLine.getToTable());
                if (Check.isEmpty(list)) {
                    list = new ArrayList<ColLine>();
                    map.put(colLine.getToTable(), list);
                }
                list.add(colLine);

                if (Check.isEmpty(colLine.getToName())) {
                    throw new VolidateException(" no match output column:" + colLine);
                }
                if (!outputTables.contains(colLine.getToTable())) {
                    throw new VolidateException(" no output table:" + colLine);
                }
                checkInputTableInfo(colLine);
            }
            checkOutputTableInfo(outputTables, map);
        }


    }


    private void checkOutputTableInfo(Set<String> outputTables, Map<String, List<ColLine>> map) {
        Map<String, List<String>> dbMap = new HashMap<String, List<String>>();
        for (String table : outputTables) {
            List<String> list = MetaCache.getInstance().getColumnByDBAndTable(table);
            dbMap.put(table, list);
        }

        for (Entry<String, List<ColLine>> entry : map.entrySet()) {
            String table = entry.getKey();
            List<ColLine> pList = entry.getValue();
            List<String> dList = dbMap.get(table);
            if (Check.isEmpty(dList)) {
                throw new VolidateException(" meta data table '" + table + "'" + " has no column.");
            }
            if (pList.size() != dList.size()) {
                throw new VolidateException(" column number/types are different '" + table + "': Table insclause-0 has " + dList.size() + " columns, but query has " + pList.size() + " columns.");
            }
        }
    }

    private void checkInputTableInfo(ColLine colLine) {
        Set<String> fromNameSet = colLine.getFromNameSet();
        for (String fromName : fromNameSet) {
            if (NumberUtil.isNumeric(fromName)) {
                continue;
            }

            int lastIndexOf = fromName.lastIndexOf('.');
            String column = fromName.substring(lastIndexOf + 1);
            String table = fromName.substring(0, lastIndexOf);
            List<String> list = initAndGet(table);
            boolean tableNoCol = true;
            if (Check.notEmpty(list)) {
                for (String string : list) {
                    if (column.equalsIgnoreCase(string)) {
                        tableNoCol = false;
                    }
                }
            }
            if (tableNoCol) {
                throw new VolidateException(" input table " + table + " no this column:" + column);
            }
        }
    }

    private List<String> initAndGet(String table) {
        List<String> list2 = dbMap.get(table);
        if (Check.isEmpty(list2)) {
            List<String> list = MetaCache.getInstance().getColumnByDBAndTable(table);
            dbMap.put(table, list);
            list2 = list;
        }
        return list2;
    }
}

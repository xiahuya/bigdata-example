package com.xhjava.sql.parse.bean;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Liaopan on 2019/3/13.
 */
@Data
@ToString
public class Sql {
    private String targetTable;
    private LinkedList<SqlSelectColumn> selectColumns = new LinkedList<>();
    private String sourceSchema;
    private String sourceTable;
    private String sourceTableAliasName;
    private LinkedList<JoinTable> joinTables = new LinkedList<>();

    private String where;

    private String groupBy;

    public void addColumn(SqlSelectColumn column) {
        this.selectColumns.add(column);
    }

    public void addJoinTable(JoinTable table, int index) {
        if (index < 0) {
            this.joinTables.addFirst(table);
        } else if (index == 0) {
            this.joinTables.addLast(table);
        } else {
            this.joinTables.add(index, table);
        }
    }

    @Data
    @Builder
    public static class SqlSelectColumn {
        private String columnExpr;
        private String aliasName;
    }

    @Data
    @Builder
    public static class JoinTable {
        private String owner;
        private String tableName;
        private String fullTableName;
        private String tableExpr;
        private String aliasName;
        private String joinType;
        private String joinOnCondition;
    }


}

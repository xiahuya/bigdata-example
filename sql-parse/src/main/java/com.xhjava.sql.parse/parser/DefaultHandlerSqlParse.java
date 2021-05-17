package com.xhjava.sql.parse.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.xhjava.sql.parse.bean.Sql;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Liaopan on 2019/11/25.
 */
public abstract class DefaultHandlerSqlParse implements ParseSql {

    private SqlType sqlType;

    protected DefaultHandlerSqlParse(SqlType sqlType) {
        this.sqlType = sqlType;
    }
    @Override
    public final Sql parseSql(String sql){
        List<SQLStatement> statementList = parseStatements(sql);
        return handlerParse(statementList);
    }

    public abstract List<SQLStatement> parseStatements(String sql);

    protected Sql handlerParse(List<SQLStatement> statementList) {
        Sql sqlObject = new Sql();
        for (int i = 0, size = statementList.size(); i < size; i++) {
            SQLStatement stmt = statementList.get(i);
            SQLSelectQueryBlock mySqlSelectQueryBlock = null;
            if (stmt instanceof SQLInsertStatement) {
                SQLInsertStatement insertStatement = (SQLInsertStatement) stmt;
                sqlObject.setTargetTable(insertStatement.getTableSource().getExpr().toString());
                mySqlSelectQueryBlock = (SQLSelectQueryBlock) insertStatement.getQuery().getQuery();
            } else if (stmt instanceof SQLSelectStatement) {
                mySqlSelectQueryBlock = (SQLSelectQueryBlock) ((SQLSelectStatement) stmt).getSelect().getQuery();
            }

            assert mySqlSelectQueryBlock != null;
            switch (sqlType) {
                case SQLSERVER:
                    mySqlSelectQueryBlock = (SQLSelectQueryBlock) mySqlSelectQueryBlock;
                    break;
                case ORACLE:
                    mySqlSelectQueryBlock = (OracleSelectQueryBlock) mySqlSelectQueryBlock;
                    break;
                case MYSQL:
                    mySqlSelectQueryBlock = (MySqlSelectQueryBlock) mySqlSelectQueryBlock;
                    break;
            }
            List<SQLSelectItem> selectList = mySqlSelectQueryBlock.getSelectList();
            // 处理select 列
            for (SQLSelectItem item : selectList) {
                String aliasName = item.getAlias();
                String selectColumn = StringUtils.replacePattern(item.toString(), "AS\\s{1}" + aliasName, " ");
                sqlObject.addColumn(Sql.SqlSelectColumn.builder().columnExpr(selectColumn).aliasName(item.getAlias()).build());
            }
            // 处理 from 表
            funJoinTable(mySqlSelectQueryBlock.getFrom(), sqlObject);

            // 处理 where
            if (mySqlSelectQueryBlock.getWhere() != null) {
                if(mySqlSelectQueryBlock.getWhere()  instanceof SQLInListExpr){
                    SQLInListExpr where = (SQLInListExpr)mySqlSelectQueryBlock.getWhere();
                    StringBuilder whereBuilder = new StringBuilder("");
                    whereBuilder.append(where.getExpr())
                            .append(where.isNot() ? " not in ": " in ")
                            .append(String.join(",",
                                    where.getTargetList().stream()
                                            .map(a -> ((SQLCharExpr)a).getText()).collect(Collectors.toList())));
                    sqlObject.setWhere(whereBuilder.toString());

                }else {
                    sqlObject.setWhere(mySqlSelectQueryBlock.getWhere().toString());
                }

            }

        }
        return sqlObject;
    }

    private static void funJoinTable(SQLTableSource joinTableSource, Sql sqlObject) {

        if (joinTableSource instanceof SQLExprTableSource) { // 主表
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) joinTableSource;
            SQLPropertyExpr expr = (SQLPropertyExpr) sqlExprTableSource.getExpr();
            sqlObject.setSourceSchema(expr.getOwnernName());
            sqlObject.setSourceTable(expr.getName());
            sqlObject.setSourceTableAliasName(sqlExprTableSource.getAlias());
        } else if(joinTableSource instanceof SQLSubqueryTableSource) { // join table
            SQLSubqueryTableSource sqlExprTableSource = (SQLSubqueryTableSource) joinTableSource;
            sqlObject.setSourceSchema(null);
            sqlObject.setSourceTable(sqlExprTableSource.toString());
            sqlObject.setSourceTableAliasName(sqlExprTableSource.getAlias());
        }else{
            SQLJoinTableSource joinTable = (SQLJoinTableSource) joinTableSource;
            SQLTableSource rightTableSource = joinTable.getRight();
            if(rightTableSource instanceof SQLSubqueryTableSource){
                SQLSubqueryTableSource rightTable = (SQLSubqueryTableSource)rightTableSource;
                String tableName = rightTable.getSelect().getQuery().toString().replaceAll("\n"," ");
                /**
                 * ((SQLPropertyExpr)((SQLExprTableSource)
                 *                         ((MySqlSelectQueryBlock) rightTable.getSelect().getQuery()).getFrom()).getExpr()).getOwnernName()
                 */
                sqlObject.addJoinTable(Sql.JoinTable.builder().owner(null)
                        .tableName(null)
                        .fullTableName(null)
                        .tableExpr("("+tableName + ")")
                        .aliasName(rightTable.getAlias())
                        .joinType(joinTable.getJoinType().name)
                        .joinOnCondition(joinTable.getCondition().toString()).build(), -1);
            }else if(rightTableSource instanceof SQLExprTableSource) {
                SQLExprTableSource rightTable = (SQLExprTableSource) rightTableSource;
                sqlObject.addJoinTable(Sql.JoinTable.builder().owner(((SQLPropertyExpr) rightTable.getExpr()).getOwnernName())
                        .tableName(((SQLPropertyExpr) rightTable.getExpr()).getName())
                        .fullTableName(rightTable.getExpr().toString())
                        .aliasName(rightTable.getAlias())
                        .joinType(joinTable.getJoinType().name)
                        .joinOnCondition(joinTable.getCondition().toString()).build(), -1);
            }


            funJoinTable(joinTable.getLeft(), sqlObject);
        }
    }
}

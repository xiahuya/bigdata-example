package com.xhjava.sql.parse.parser;

import com.xhjava.sql.parse.bean.Sql;
import com.xhjava.sql.parse.ccj.JoinTableFromItemVisitor;
import com.xhjava.sql.parse.ccj.PrimaryTableFromItemVisitor;
import com.xhjava.sql.parse.ccj.SelectItemVisitor;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;

/**
 * Created by Liaopan on 2020/8/6 0006.
 */
@Slf4j
public class CCJHandlerSqlParse implements ParseSql {

    @Override
    public Sql parseSql(String sql) {
        Sql sqlObj = new Sql();
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (statement instanceof Insert) {
                Insert insertStatement = (Insert) statement;

                sqlObj.setTargetTable(insertStatement.getTable().getFullyQualifiedName());

                handlerSelectSql(insertStatement.getSelect(), sqlObj);
            } else if (statement instanceof Select) {
                handlerSelectSql((Select) statement, sqlObj);
            } else {
                throw new NotImplementedException("暂不支持！");
            }
        } catch (Exception e) {
            log.error("解析sql 出错！", e);
        }
        return sqlObj;
    }

    private void handlerSelectSql(Select selectQuery, Sql sqlObj) {

        LinkedList<Sql.SqlSelectColumn> selectColumns = new LinkedList<>();
        sqlObj.setSelectColumns(selectColumns);

        SelectItemVisitor selectItemVisitor = new SelectItemVisitor(selectColumns);

        selectQuery.getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                // 处理 select 的列
                Optional.ofNullable(plainSelect.getSelectItems()).orElse(Collections.emptyList())
                        .forEach(selectItem -> {
                            selectItem.accept(selectItemVisitor);
                        });

                // 处理from
                final FromItem fromItem = plainSelect.getFromItem();
                if (fromItem != null) {
                    fromItem.accept(new PrimaryTableFromItemVisitor(sqlObj));
                }
                // 处理 join tables :
                handlerJoinTable(plainSelect.getJoins(), sqlObj);

                // 处理 where
                sqlObj.setWhere("" + plainSelect.getWhere());

                // 处理groupby
                sqlObj.setGroupBy(PlainSelect.getStringList(Optional.ofNullable(plainSelect.getGroupByColumnReferences())
                        .orElse(Collections.emptyList()))
                );
            }
        });
    }

    public void handlerJoinTable(List<Join> joins, Sql sqlObj) {
        Optional.ofNullable(joins).orElse(Collections.emptyList()).forEach(join -> {
            final Sql.JoinTable.JoinTableBuilder builder = Sql.JoinTable.builder();
            join.getRightItem().accept(new JoinTableFromItemVisitor(builder));
            builder.joinType(joinType(join));
            builder.joinOnCondition("" + join.getOnExpression());
            sqlObj.addJoinTable(builder.build(), 0);
        });
    }

    private String joinType(Join join) {
        if (join.isSimple() && join.isOuter()) {
            return "OUTER ";
        } else if (join.isSimple()) {
            return "";
        } else {
            String type = "";
            if (join.isRight()) {
                type = type + "RIGHT ";
            } else if (join.isNatural()) {
                type = type + "NATURAL ";
            } else if (join.isFull()) {
                type = type + "FULL ";
            } else if (join.isLeft()) {
                type = type + "LEFT ";
            } else if (join.isCross()) {
                type = type + "CROSS ";
            }

            if (join.isOuter()) {
                type = type + "OUTER ";
            } else if (join.isInner()) {
                type = type + "INNER ";
            } else if (join.isSemi()) {
                type = type + "SEMI ";
            }


            type = type + "JOIN ";


            return type;
        }
    }
}

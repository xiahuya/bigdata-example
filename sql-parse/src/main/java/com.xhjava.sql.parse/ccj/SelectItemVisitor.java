package com.xhjava.sql.parse.ccj;

import com.xhjava.sql.parse.bean.Sql;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.util.List;
import java.util.Optional;

/**
 * Created by Liaopan on 2020/8/11 0011.
 */
public class SelectItemVisitor extends SelectItemVisitorAdapter {
    List<Sql.SqlSelectColumn> selectColumns;

    public SelectItemVisitor(List<Sql.SqlSelectColumn> selectColumns) {
        this.selectColumns = selectColumns;
    }
    @Override
    public void visit(SelectExpressionItem item) {
        selectColumns.add(Sql.SqlSelectColumn.builder()
                .aliasName(Optional.ofNullable(item.getAlias()).map(Alias::getName).orElse(""))
                .columnExpr(item.getExpression().toString()).build());
    }
}

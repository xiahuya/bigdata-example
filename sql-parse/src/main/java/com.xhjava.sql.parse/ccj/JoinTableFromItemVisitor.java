package com.xhjava.sql.parse.ccj;

import com.xhjava.sql.parse.bean.Sql;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.Optional;

/**
 * 解析主表
 * Created by Liaopan on 2020/8/11 0011.
 */
@Slf4j
public class JoinTableFromItemVisitor extends PrimaryTableFromItemVisitor {
    private Sql.JoinTable.JoinTableBuilder builder;

    public JoinTableFromItemVisitor(Sql.JoinTable.JoinTableBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void visit(Table table) {
        log.debug("Visit Table:" + table);
        builder.owner(table.getSchemaName()).tableName(table.getName())
                .fullTableName(table.getFullyQualifiedName())
                .aliasName(Optional.ofNullable(table.getAlias()).map(Alias::getName).orElse(""));
    }

    @Override
    public void visit(SubSelect subSelect) {
        log.debug("Visit SubSelect:" + subSelect);
        builder.tableExpr(subSelect.getSelectBody().toString());
        builder.aliasName(Optional.ofNullable(subSelect.getAlias()).map(Alias::getName).orElse(""));
    }

    @Override
    public void visit(SubJoin subjoin) {
        log.debug("Visit SubJoin:" + subjoin);
        super.visit(subjoin);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        log.debug("Visit LateralSubSelect:" + lateralSubSelect);
        super.visit(lateralSubSelect);
    }

    @Override
    public void visit(ValuesList valuesList) {
        log.debug("Visit ValuesList:" + valuesList);
        super.visit(valuesList);
    }

    @Override
    public void visit(TableFunction valuesList) {
        log.debug("Visit TableFunction:" + valuesList);
        super.visit(valuesList);
    }

}

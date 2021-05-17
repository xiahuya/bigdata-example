package com.xhjava.sql.parse.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * Created by Liaopan on 2018/11/13.
 */
public class SqlServerSqlParse extends DefaultHandlerSqlParse {

    public SqlServerSqlParse() {
        super(SqlType.SQLSERVER);
    }

    @Override
    public List<SQLStatement> parseStatements(String sql) {
        return SQLUtils.parseStatements(sql, JdbcConstants.SQL_SERVER);
    }
}

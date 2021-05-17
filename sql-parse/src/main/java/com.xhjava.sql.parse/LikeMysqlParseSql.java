package com.xhjava.sql.parse;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.xhjava.sql.parse.parser.DefaultHandlerSqlParse;

import java.util.List;

/**
 * Created by Liaopan on 2018/11/13.
 */
public class LikeMysqlParseSql extends DefaultHandlerSqlParse {

    public LikeMysqlParseSql() {
        super(SqlType.MYSQL);
    }
    @Override
    public List<SQLStatement> parseStatements(String sql) {
        return SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
    }
}

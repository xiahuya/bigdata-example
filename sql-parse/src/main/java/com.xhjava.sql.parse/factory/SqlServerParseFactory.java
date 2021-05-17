package com.xhjava.sql.parse.factory;

import com.xhjava.sql.parse.parser.ParseSql;
import com.xhjava.sql.parse.parser.SqlServerSqlParse;

/**
 * Created by Liaopan on 2019/6/28.
 */
public class SqlServerParseFactory implements SqlParseFactory{
    @Override
    public ParseSql createSqlParse() {
        return new SqlServerSqlParse();
    }
}

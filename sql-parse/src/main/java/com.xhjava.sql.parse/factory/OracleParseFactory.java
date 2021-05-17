package com.xhjava.sql.parse.factory;

import com.xhjava.sql.parse.parser.OracleSqlParse;
import com.xhjava.sql.parse.parser.ParseSql;

/**
 * Created by Liaopan on 2019/6/28.
 */
public class OracleParseFactory implements SqlParseFactory{
    @Override
    public ParseSql createSqlParse() {
        return new OracleSqlParse();
    }
}

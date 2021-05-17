package com.xhjava.sql.parse.factory;

import com.xhjava.sql.parse.parser.ParseSql;

/**
 * Created by Liaopan on 2018/11/15.
 */
public interface SqlParseFactory {
    ParseSql createSqlParse();
}

package com.xhjava.sql.parse.factory;

import com.xhjava.sql.parse.parser.CCJHandlerSqlParse;
import com.xhjava.sql.parse.parser.ParseSql;

/**
 * Created by Liaopan on 2020/8/6 0006.
 */
public class CCJSqlParseFactory implements SqlParseFactory {

    @Override
    public ParseSql createSqlParse() {
        return new CCJHandlerSqlParse();
    }
}

package com.xhjava.sql.parse.parser;


import com.xhjava.sql.parse.bean.Sql;

/**
 * Created by Liaopan on 2018/11/13.
 */
public interface ParseSql {

    enum SqlType {
        MYSQL(1,"mysql"),
        SQLSERVER(2,"mssql"),
        ORACLE(3,"oracle"),
        DEFAULT(4,"default");

        SqlType(int index, String name){}

    }

    Sql parseSql(String sql);
}

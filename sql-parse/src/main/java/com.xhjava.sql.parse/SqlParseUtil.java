package com.xhjava.sql.parse;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xhjava.sql.parse.bean.Sql;
import com.xhjava.sql.parse.factory.CCJSqlParseFactory;
import com.xhjava.sql.parse.factory.MysqlParseFactory;
import com.xhjava.sql.parse.factory.OracleParseFactory;
import com.xhjava.sql.parse.factory.SqlServerParseFactory;
import com.xhjava.sql.parse.parser.ParseSql;

import static com.xhjava.sql.parse.parser.ParseSql.SqlType.MYSQL;


/**
 * Created by Liaopan on 2018/7/19.
 */
public class SqlParseUtil {

    private static ParseSql instance = new MysqlParseFactory().createSqlParse();

    public static Sql parseSql(String sql, ParseSql.SqlType type) throws Exception {
        switch (type) {
            case ORACLE:
                instance = new OracleParseFactory().createSqlParse();
                break;
            case MYSQL:
                instance = new MysqlParseFactory().createSqlParse();
                break;
            case SQLSERVER:
                instance = new SqlServerParseFactory().createSqlParse();
                break;
            default:
                instance = new CCJSqlParseFactory().createSqlParse();
                break;
        }
        return instance.parseSql(sql);
    }

    public static void main(String[] args) {

        String sql = "select\n" +
                "yy.id  AS hospitalno ---  医疗机构编码 string 'pk',\n" +
                ",yy.name  AS hospitalname ---  医疗机构名称 string \n" +
                ",nvl(regexp_replace(rtrim(gh.xh),'NULL',''),'') as  visitnumber  --- 门诊就诊号 √\n" +
                ",nvl(regexp_replace(rtrim(gh.patid),'NULL',''),'') as  patientid  --- 患者标识 \n" +
                ",nvl(regexp_replace(rtrim(gh.hzxm),'NULL',''),'') as  patientname  --- 患者姓名 \n" +
                ",case when  rtrim(br.sex)='男' then '1' when rtrim(br.sex)='女' then '2' else '9' end as  sexcode  \n" +
                "from  (select row_number()over(partition by xh order by lastupdatedttm desc) as RN,* from  zzx_his_repl.vm_ghzdk) gh     --挂号账单库\n" +
                "inner JOIN zzx_his_repl.SF_BRXXK br ON gh.patid = br.patid  and br.isdeleted='0' --病人信息库\n" +
                "LEFT JOIN zzx_his_repl.YY_YBFLK yb ON gh.ybdm = yb.ybdm  and yb.isdeleted='0'  --医保分类库\n" +
                "LEFT JOIN zzx_his_repl.czryk staff1 ON gh.czyh =staff1.id and staff1.isdeleted='0'  --操作员编码库\n" +
                "left join zzx_his_repl.yy_jbconfig yy on yy.isdeleted='0'\n" +
                "WHERE gh.RN in (select a from b)";
        try {
            Sql obj = SqlParseUtil.parseSql(sql, MYSQL);
            String s = JSON.toJSONString(obj);
            System.out.println(s);
            /*System.out.println("插入到表：" + obj.getTargetTable());
            System.out.println("查询库：" + obj.getSourceSchema() + " , 表" + obj.getSourceTable() + ", 别名： " + obj.getSourceTableAliasName());
            System.out.println("查询列：--------- select ------------");
            int i = 1;
            for (Sql.SqlSelectColumn column : obj.getSelectColumns()) {
                System.out.print(i++ + ". 列值：" + column.getColumnExpr() + ", ");
                System.out.println("目标列：" + column.getAliasName());
                System.out.println("------------------------");
            }
            System.out.println("------------ from -----------");
            System.out.println("主表： " + obj.getSourceSchema() + " . " + obj.getSourceTable() + " as " + obj.getSourceTableAliasName());
            for (Sql.JoinTable table : obj.getJoinTables()) {
                System.out.println("使用 " + table.getJoinType() + "  连接库 " + table.getOwner() + " , 表：" +
                        table.getTableName() + " , 全名：" + table.getFullTableName() + ","+table.getTableExpr()+", 别名： " + table.getAliasName() +
                        " 连接条件： " + table.getJoinOnCondition());
            }

            System.out.println("---------- where ---------");
            System.out.println(obj.getWhere());*/
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

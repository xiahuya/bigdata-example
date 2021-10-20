package cn.xhjava;

import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author Xiahu
 * @create 2020/6/5
 */


@AggregationFunction("_myUDF")
@ScalarFunction
@Description("hive to_date function")
public class PrestoUdf {
    /*private PrestoUdf() {
    }

    public static final String DATE_FORMAT = "yyyy-MM-dd";


    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_date(@SqlType(StandardTypes.BIGINT) long input) {
        final DateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return Slices.utf8Slice(format.format(new Date(input)));

    }*/



}

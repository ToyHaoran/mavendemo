package com.haoran;

import com.haoran.bean.User;
import com.haoran.utils.ConnectUtil;
import lombok.val;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Test02 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession ss = new ConnectUtil().getSS();

        Dataset<Row> ds = ss.read().json("input/user.json");
        ds.cache();
        ds.show(30,false);


        ss.stop();
    }

}

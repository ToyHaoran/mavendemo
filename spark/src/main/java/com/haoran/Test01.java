package com.haoran;

import com.haoran.utils.ConnectUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;


public class Test01 {

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {

        // 1. 创建sparkContext
        JavaSparkContext sc = new ConnectUtil().getSC();

        // 2. 编写代码
        // 找出幸运数字
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(4, 56, 7, 8, 1, 2));
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);  // 幸运数字

        // 创建广播变量，只发送一份数据到每一个executor
        Broadcast<List<Integer>> broadcast = sc.broadcast(list);
        JavaRDD<Integer> result = intRDD.filter(v1 -> broadcast.value().contains(v1));
        result.collect().forEach(System.out::println);

        // Thread.sleep(9999999);

        // 3. 关闭sc
        sc.stop();



    }

}

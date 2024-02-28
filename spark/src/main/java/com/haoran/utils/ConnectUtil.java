package com.haoran.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ConnectUtil {

    /**
     * 1. 创建配置对象Sparkconf
     */
    private SparkConf getConf(){
        String name = System.getProperty("user.name");
        System.out.println(name);
        SparkConf conf = null;
        if (name.equals("root")){
            System.out.println("集群运行，yarn模式==============");
            conf = new SparkConf().setMaster("yarn").setAppName(name);
            // 替换默认的序列化机制
            // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // 注册需要使用kryo序列化的自定义类
            // .registerKryoClasses(new Class[]{Class.forName("com.haoran.bean.User")});
        }else{
            System.out.println("本地运行===============");
            conf = new SparkConf().setMaster("local[*]").setAppName(name);
        }
        return conf;
    }

    /**
     * 2. 创建JavaSparkContext
     */
    public JavaSparkContext getSC() {
        return new JavaSparkContext(getConf());
    }

    /**
     * 2. 获取sparkSession
     */
    public SparkSession getSS(){
        return SparkSession.builder().config(getConf()).getOrCreate();
    }

    /**
     * 2. 获取支持Hive的SparkSession
     */
    public SparkSession getHiveSS(){
        return SparkSession.builder().enableHiveSupport().config(getConf()).getOrCreate();
    }


}

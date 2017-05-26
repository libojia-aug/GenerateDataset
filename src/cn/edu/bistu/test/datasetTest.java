package cn.edu.bistu.test;

import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import cn.edu.bistu.utils.IPConvert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by libojia on 16/10/31.
 */
public class datasetTest {
    private static GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();

    private static SparkConf conf = new SparkConf().setAppName("generateDataset").setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf);

    private static JavaPairRDD<String, Integer> count(JavaRDD<String> words) {
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairRDD<Integer, String> swappedPair1 = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        });
        JavaPairRDD<String, Integer> swappedPair2 = swappedPair1.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

        });
        return swappedPair2;
    }

    private static JavaPairRDD<String, Integer> ipCount(JavaRDD<String> words) {
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairRDD<Integer, String> swappedPair1 = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                IPConvert ipConvert = new IPConvert();
                return new Tuple2<>(item._2(), ipConvert.longToIp(Long.parseLong(item._1())));
            }

        });
        JavaPairRDD<String, Integer> swappedPair2 = swappedPair1.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

        });
        return swappedPair2;
    }

    public static void main(String[] args) throws Exception {

        JavaRDD<String> lines = sc.textFile(config.getOutputPath(), 1);

        JavaRDD<String> sIp = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return (s.split(parameter.SEPARATOR))[2];
            }
        });

        JavaRDD<String> dIp = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return (s.split(parameter.SEPARATOR))[4];
            }
        });

        JavaRDD<String> domain = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return (s.split(parameter.SEPARATOR))[6];
            }
        });
        JavaRDD<String> url = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return (s.split(parameter.SEPARATOR))[7];
            }
        });

        ipCount(sIp).saveAsTextFile(config.getTestOutputPath() + "/sip");
        ipCount(dIp).saveAsTextFile(config.getTestOutputPath() + "/dip");
        count(domain).saveAsTextFile(config.getTestOutputPath() + "/domain");
        count(url).saveAsTextFile(config.getTestOutputPath() + "/url");
    }
}

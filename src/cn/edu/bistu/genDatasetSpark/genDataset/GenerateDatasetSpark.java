package cn.edu.bistu.spark.genDataset;

import cn.edu.bistu.genDataset.GenerateDataset;
import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import cn.edu.bistu.utils.IPConvert;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * @author Berger_LBJ
 */
public class GenerateDatasetSpark {
    private static final String dateFormatYearMonth = "yyyy-MM";
    private static final String dateFormatDay = "yyyy-MM-dd";
    private static final String dateFormatHour = "hh";
    private static final SimpleDateFormat formatYearMonth = new SimpleDateFormat(dateFormatYearMonth);
    private static final SimpleDateFormat formatDay = new SimpleDateFormat(dateFormatDay);
    private static final SimpleDateFormat formatHour = new SimpleDateFormat(dateFormatHour);
    private static JavaSparkContext sc;

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();

        SparkConf conf = new SparkConf().setAppName("generateDataset").setMaster("local");
        sc = new JavaSparkContext(conf);

        // 声明数据集
        JavaRDD<String> sourceIp_h = sc.textFile(config.getSourceAddressIplbsFile_h());
        JavaRDD<String> sourceIp_l = sc.textFile(config.getSourceAddressIplbsFile_l());
        JavaRDD<String> destinationIp_h = sc.textFile(config.getDestinationAddressIplbsFile_h());
        JavaRDD<String> destinationIp_l = sc.textFile(config.getDestinationAddressIplbsFile_l());
        JavaRDD<String> domain_h = sc.textFile(config.getDomainFile_h());
        JavaRDD<String> domain_l = sc.textFile(config.getDomainFile_l());
        JavaRDD<String> url_h = sc.textFile(config.getUrlFile_h());
        JavaRDD<String> url_l = sc.textFile(config.getUrlFile_l());

        int count = config.getCount();
        int slices = config.getSlices();

        int sourceIpCount_h = (int) (count * config.getSourceAddressIplbsFactor());
        int destinationIpCount_h = (int) (count * config.getDestinationAddressIplbsFactor());
        int domainCount_h = (int) (count * config.getDomainFactor());
        int urlCount_h = (int) (count * config.getUrlFactor());

        PairFunction<Tuple2<String, Tuple2<String, String>>, String, String> removebracket = new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> arg0) throws Exception {
                return new Tuple2<>(arg0._1, arg0._2._1 + parameter.SEPARATOR + arg0._2._2);
            }
        };

        JavamergeData(sourceIp_h, sourceIp_l, sourceIpCount_h, count, slices)
                .join(JavamergeData(destinationIp_h, destinationIp_l, destinationIpCount_h, count, slices))
                .mapToPair(removebracket).join(JavamergeData(domain_h, domain_l, domainCount_h, count, slices))
                .mapToPair(removebracket).join(JavamergeData(url_h, url_l, urlCount_h, count, slices))
                .mapToPair(removebracket).values().map(new Function<String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public String call(String s) {
                GenerateDataBase generateData = new GenerateDataBase();
                GenerateDataset generateDataset = new GenerateDataset();
                // 时间
                String timestamp = generateDataset.generateNumber(config.getDiscontinuityPoints(),
                        config.getDistributions(), config.getAccuracy());
                generateData.setTimeStamp(timestamp);
                Date d = new Timestamp(Long.parseLong(timestamp));
                generateData.setYearMonth(formatYearMonth.format(d));
                generateData.setDay(formatDay.format(d));
                generateData.setHour(formatHour.format(d));
                String[] dataStrings = s.split(parameter.SEPARATOR);
                // ip转换
                IPConvert ipConvert = new IPConvert();
                // 源IP
                String[] sourceIplbs = dataStrings[0].split(parameter.SPACE);
                generateData.setSourceIp(ipConvert.ip2Long(sourceIplbs[0]));
                // 源IP省市
                generateData.setSourceProvince(sourceIplbs[1]);
                generateData.setSourceCity(sourceIplbs[2]);
                // 目标IP
                String[] destinationIplbs = dataStrings[1].split(parameter.SPACE);
                generateData.setDestinationIp(ipConvert.ip2Long(destinationIplbs[0]));
                // domain
                generateData.setDomain(dataStrings[2]);
                // URL
                generateData.setUrl(dataStrings[3]);
                return generateData.toString();
            }
        }).saveAsTextFile(config.getOutputPath());
        System.out.println("生成" + (count / 10000) + "万条数据, 累计耗时:" + (System.currentTimeMillis() - time) + "ms");
    }

    public static JavaPairRDD<String, String> JavamergeData(JavaRDD<String> rdd_h, JavaRDD<String> rdd_l, int count_h,
                                                            int count, int slices) {
        return sc.parallelize(rdd_h.takeSample(false, count_h), slices)
                .union(sc.parallelize(rdd_l.takeSample(false, count - count_h), slices))
                .mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer partitionId, Iterator<String> iterator) throws Exception {
                        int i = 0;
                        List<String> listTemp = new ArrayList<>(count);
                        while (iterator.hasNext()) {
                            i++;
                            listTemp.add(String.valueOf(partitionId) + parameter.COMMA + String.valueOf(i)
                                    + parameter.SEPARATOR + iterator.next());
                        }
                        return listTemp.iterator();
                    }
                }, true).mapToPair(new PairFunction<String, String, String>() {
                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, String> call(String s) {
                        String[] temp = s.split(parameter.SEPARATOR);
                        return new Tuple2<>(temp[0], temp[1]);
                    }
                });
    }
}
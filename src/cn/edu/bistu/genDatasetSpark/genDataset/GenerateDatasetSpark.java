package cn.edu.bistu.genDatasetSpark.genDataset;

import cn.edu.bistu.genDataset.GenerateDataset;
import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import cn.edu.bistu.utils.IPConvert;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


/**
 * @author Berger_LBJ
 */
public class GenerateDatasetSpark implements Serializable {
    private static final String dateFormatYearMonth = "yyyy-MM";
    private static final String dateFormatDay = "yyyy-MM-dd";
    private static final String dateFormatHour = "hh";
    private static final SimpleDateFormat formatYearMonth = new SimpleDateFormat(dateFormatYearMonth);
    private static final SimpleDateFormat formatDay = new SimpleDateFormat(dateFormatDay);
    private static final SimpleDateFormat formatHour = new SimpleDateFormat(dateFormatHour);
    // log
    public static Logger logger = Logger.getLogger("main");

    private static int getExtractCount(double factor, int count) {
        int i;
        for (i = 1; i < count; i++) {
            double countTemp = i * factor;
            if (countTemp == (int) countTemp) {
                break;
            }
        }
        return count / i;
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        logger.info("Start: " + time);

        // 调用配置文件
        GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();
        // 获取生成总数
        int count = config.getCount();

        int sourceIpExtractCount = getExtractCount(config.getSourceAddressIplbsFactor(), count);
        int destinationIpExtractCount = getExtractCount(config.getDestinationAddressIplbsFactor(), count);
        int domainExtractCount = getExtractCount(config.getDomainFactor(), count);
        int urlExtractCount = getExtractCount(config.getUrlFactor(), count);
        RDDAction RDDAction = new RDDAction();
        SparkConf conf = new SparkConf().setAppName("generateDataset");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 生成高频低频广播变量
        Broadcast<List<String>> sourceIp_h = RDDAction.loadBroadcast(sc, config.getSourceAddressIplbsFile_h(), sourceIpExtractCount);
        Broadcast<List<String>> sourceIp_l = RDDAction.loadBroadcast(sc, config.getSourceAddressIplbsFile_l(), count - sourceIpExtractCount);
        Broadcast<List<String>> destinationIp_h = RDDAction.loadBroadcast(sc, config.getDestinationAddressIplbsFile_h(), destinationIpExtractCount);
        Broadcast<List<String>> destinationIp_l = RDDAction.loadBroadcast(sc, config.getDestinationAddressIplbsFile_l(), count - destinationIpExtractCount);
        Broadcast<List<String>> domain_h = RDDAction.loadBroadcast(sc, config.getDomainFile_h(), domainExtractCount);
        Broadcast<List<String>> domain_l = RDDAction.loadBroadcast(sc, config.getDomainFile_l(), count - domainExtractCount);
        Broadcast<List<String>> url_h = RDDAction.loadBroadcast(sc, config.getUrlFile_h(), urlExtractCount);
        Broadcast<List<String>> url_l = RDDAction.loadBroadcast(sc, config.getUrlFile_l(), count - urlExtractCount);

        // 分片数
        int slices = config.getSlices();
        // 相应集合高频数
        int sourceIpCount_h = (int) (count * config.getSourceAddressIplbsFactor());
        int destinationIpCount_h = (int) (count * config.getDestinationAddressIplbsFactor());
        int domainCount_h = (int) (count * config.getDomainFactor());
        int urlCount_h = (int) (count * config.getUrlFactor());
        // 生成数据，高低频数据集整合、去括号
        RDDAction.JavamergeData(sc, sourceIp_h, sourceIp_l, sourceIpCount_h, count, slices)
                .join(RDDAction.JavamergeData(sc, destinationIp_h, destinationIp_l, destinationIpCount_h, count, slices))
                .mapToPair(RDDAction.removebracket).join(RDDAction.JavamergeData(sc, domain_h, domain_l, domainCount_h, count, slices))
                .mapToPair(RDDAction.removebracket).join(RDDAction.JavamergeData(sc, url_h, url_l, urlCount_h, count, slices))
                .mapToPair(RDDAction.removebracket).values().map(new Function<String, String>() {
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
        logger.info("Finish: 生成" + (count / 10000) + "万条数据, 累计耗时:" + (System.currentTimeMillis() - time) + "ms");
    }
}
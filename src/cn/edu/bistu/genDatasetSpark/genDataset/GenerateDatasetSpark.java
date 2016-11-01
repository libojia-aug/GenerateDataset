package cn.edu.bistu.genDatasetSpark.genDataset;

import cn.edu.bistu.genDataset.GenerateDataset;
import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import cn.edu.bistu.utils.IPConvert;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    // log
    public static Logger logger = Logger.getLogger("main");

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        logger.info("Start: " + time);
        // 调用配置文件
        GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();
        // 生成高频低频广播变量
        Broadcast<List<String>> sourceIp_h = RDDAction.loadBroadcast(config.getSourceAddressIplbsFile(), config.getSourceIplbs_hFileExtractCount());
        Broadcast<List<String>> sourceIp_l = RDDAction.loadBroadcast(config.getSourceAddressIplbsFile(), config.getSourceIplbs_lFileExtractCount());
        Broadcast<List<String>> destinationIp_h = RDDAction.loadBroadcast(config.getDestinationAddressIplbsFile(), config.getDestinationIplbs_hFileExtractCount());
        Broadcast<List<String>> destinationIp_l = RDDAction.loadBroadcast(config.getDestinationAddressIplbsFile(), config.getDestinationIplbs_lFileExtractCount());
        Broadcast<List<String>> domain_h = RDDAction.loadBroadcast(config.getDomainFile(), config.getDomain_hFileExtractCount());
        Broadcast<List<String>> domain_l = RDDAction.loadBroadcast(config.getDomainFile(), config.getDomain_lFileExtractCount());
        Broadcast<List<String>> url_h = RDDAction.loadBroadcast(config.getUrlFile(), config.getUrl_hFileExtractCount());
        Broadcast<List<String>> url_l = RDDAction.loadBroadcast(config.getUrlFile(), config.getUrl_lFileExtractCount());
        // 获取生成总数
        int count = config.getCount();
        // 分片数
        int slices = config.getSlices();
        // 相应集合高频数
        int sourceIpCount_h = (int) (count * config.getSourceAddressIplbsFactor());
        int destinationIpCount_h = (int) (count * config.getDestinationAddressIplbsFactor());
        int domainCount_h = (int) (count * config.getDomainFactor());
        int urlCount_h = (int) (count * config.getUrlFactor());
        // 生成数据，高低频数据集整合、去括号
        RDDAction.JavamergeData(sourceIp_h, sourceIp_l, sourceIpCount_h, count, slices)
                .join(RDDAction.JavamergeData(destinationIp_h, destinationIp_l, destinationIpCount_h, count, slices))
                .mapToPair(RDDAction.removebracket).join(RDDAction.JavamergeData(domain_h, domain_l, domainCount_h, count, slices))
                .mapToPair(RDDAction.removebracket).join(RDDAction.JavamergeData(url_h, url_l, urlCount_h, count, slices))
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
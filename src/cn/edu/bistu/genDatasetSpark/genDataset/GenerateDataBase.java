package cn.edu.bistu.spark.genDataset;

import java.io.Serializable;
import cn.edu.bistu.genDataset.config.parameter;

/**
 * Created by libojia on 16/10/24.
 */

public class GenerateDataBase implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    // 时间
    private String timeStamp = parameter.TIMESTAMP_ZORE;
    // 配置ID
    private int id = parameter.INT_ZORE;
    // 源地址IP
    private long sourceIp = parameter.LONG_ZORE;
    // 源地址端口
    private int sourcePort = parameter.INT_80;
    // 目标地址IP
    private long destinationIp = parameter.LONG_ZORE;
    // 目标地址端口
    private int destinationPort = parameter.INT_ZORE;
    // 域名
    private String domain = parameter.STRING_NULL;
    // URL
    private String url = parameter.STRING_NULL;

    private long xxIp = parameter.LONG_ZORE;
    private int xxPort = parameter.INT_ZORE;
    private String xx1 = parameter.STRING_NULL;
    private String xx2 = parameter.STRING_NULL;
    private long xx1Ip = parameter.LONG_ZORE;
    // ISP
    private String isp = parameter.STRING_NULL;
    // 源地址国家
    private String sourceCountry = parameter.STRING_NULL;
    // 源地址省份
    private String sourceProvince = parameter.STRING_NULL;
    // 源地址城市
    private String sourceCity = parameter.STRING_NULL;
    // 源地址网络
    private String sourceNet = parameter.STRING_NULL;
    // 年月2014-09
    private String yearMonth = parameter.YEARMONTH_DEFAULT;
    // 年月日2014-09-01
    private String day = parameter.DAY_DEFAULT;
    // 时00
    private String hour = parameter.HOUR_DEFAULT;

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(long sourceIp) {
        this.sourceIp = sourceIp;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public long getDestinationIp() {
        return destinationIp;
    }

    public void setDestinationIp(long destinationIp) {
        this.destinationIp = destinationIp;
    }

    public int getDestinationPort() {
        return destinationPort;
    }

    public void setDestinationPort(int destinationPort) {
        this.destinationPort = destinationPort;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getXxIp() {
        return xxIp;
    }

    public void setXxIp(long xxIp) {
        this.xxIp = xxIp;
    }

    public int getXxPort() {
        return xxPort;
    }

    public void setXxPort(int xxPort) {
        this.xxPort = xxPort;
    }

    public String getXx1() {
        return xx1;
    }

    public void setXx1(String xx1) {
        this.xx1 = xx1;
    }

    public String getXx2() {
        return xx2;
    }

    public void setXx2(String xx2) {
        this.xx2 = xx2;
    }

    public long getXx1Ip() {
        return xx1Ip;
    }

    public void setXx1Ip(long xx1Ip) {
        this.xx1Ip = xx1Ip;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getSourceCountry() {
        return sourceCountry;
    }

    public void setSourceCountry(String sourceCountry) {
        this.sourceCountry = sourceCountry;
    }

    public String getSourceProvince() {
        return sourceProvince;
    }

    public void setSourceProvince(String sourceProvince) {
        this.sourceProvince = sourceProvince;
    }

    public String getSourceCity() {
        return sourceCity;
    }

    public void setSourceCity(String sourceCity) {
        this.sourceCity = sourceCity;
    }

    public String getSourceNet() {
        return sourceNet;
    }

    public void setSourceNet(String sourceNet) {
        this.sourceNet = sourceNet;
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String toString() {
        return getTimeStamp() + parameter.SEPARATOR + getId() + parameter.SEPARATOR + getSourceIp() + parameter.SEPARATOR
                + getSourcePort() + parameter.SEPARATOR + getDestinationIp() + parameter.SEPARATOR + getDestinationPort()
                + parameter.SEPARATOR + getDomain() + parameter.SEPARATOR + getUrl() + parameter.SEPARATOR
                + getXxIp() + parameter.SEPARATOR + getXxPort() + parameter.SEPARATOR + getXx1() + parameter.SEPARATOR
                + getXx2() + parameter.SEPARATOR + getXx1Ip() + parameter.SEPARATOR + getIsp() + parameter.SEPARATOR
                + getSourceCountry() + parameter.SEPARATOR + getSourceProvince() + parameter.SEPARATOR + getSourceCity()
                + parameter.SEPARATOR + getSourceNet() + parameter.SEPARATOR + getYearMonth() + parameter.SEPARATOR + getDay()
                + parameter.SEPARATOR + getHour() + parameter.SEPARATOR;
    }
}

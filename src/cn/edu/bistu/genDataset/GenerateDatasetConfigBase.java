package cn.edu.bistu.genDataset;

import cn.edu.bistu.genDataset.config.parameter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

//基础配置读取
public class GenerateDatasetConfigBase implements Serializable {

    private volatile Properties properties = getGenerateDatasetProperties();

    public static Properties getGenerateDatasetProperties() {
        Properties conf1 = new Properties();
        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(parameter.GENERATE_DATASET_CONF_PROPERTIES_PATH + parameter.GENERATE_DATASET_CONF_PROPERTIES_FILE);
            if (!fs.exists(path)) {
                throw new RuntimeException("fail to locate " + parameter.GENERATE_DATASET_CONF_PROPERTIES_PATH + parameter.GENERATE_DATASET_CONF_PROPERTIES_FILE);
            }
            FSDataInputStream is = fs.open(path);
            conf1.load(is);
            IOUtils.closeQuietly(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return conf1;
    }

    final protected String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {
        final String property = System.getProperty(prop);
        return property != null ? property : properties.getProperty(prop, dft);
    }

    public int getCount() {
        return Integer.parseInt(getOptional("count"));
    }

    public int getSlices() {
        return Integer.parseInt(getOptional("slices"));
    }

    public double[] doubleArray(String parameterName) {
        String[] strArray = getOptional(parameterName).split(parameter.COMMA);
        double[] doubleArray = new double[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            doubleArray[i] = Double.parseDouble(strArray[i]);
        }
        return doubleArray;
    }

    public double[] getDiscontinuityPoints() {
        return doubleArray("discontinuity.point");
    }

    public double[] getDistributions() {
        return doubleArray("distribution");
    }

    public double getAccuracy() {
        return Double.parseDouble(getOptional("accuracy"));
    }

    public String getOutputPath() {
        return getOptional("output.path");
    }

    public String getTestOutputPath() {
        return getOptional("test.output.path");
    }

    public String getOutputFile() {
        return getOptional("output.file");
    }

    public String getSourceAddressIplbsFile_h() {
        return getOptional("input.source_address_iplbs_h.file");
    }

    public String getSourceAddressIplbsFile_l() {
        return getOptional("input.source_address_iplbs_l.file");
    }

    public double getSourceAddressIplbsFactor() {
        return Double.parseDouble(getOptional("source_address_iplbs.factor"));
    }

    public String getDestinationAddressIplbsFile_h() {
        return getOptional("input.destination_address_iplbs_h.file");
    }

    public String getDestinationAddressIplbsFile_l() {
        return getOptional("input.destination_address_iplbs_l.file");
    }

    public double getDestinationAddressIplbsFactor() {
        return Double.parseDouble(getOptional("destination_address_iplbs.factor"));
    }

    public String getDomainFile_h() {
        return getOptional("input.domain_h.file");
    }

    public String getDomainFile_l() {
        return getOptional("input.domain_l.file");
    }

    public double getDomainFactor() {
        return Double.parseDouble(getOptional("domain.factor"));
    }

    public String getUrlFile_h() {
        return getOptional("input.url_h.file");
    }

    public String getUrlFile_l() {
        return getOptional("input.url_l.file");
    }

    public double getUrlFactor() {
        return Double.parseDouble(getOptional("url.factor"));
    }

    public String getSourceAddressIplbsFile() {
        return getOptional("input.source_address_iplbs.file");
    }

    public String getDestinationAddressIplbsFile() {
        return getOptional("input.destination_address_iplbs.file");
    }

    public String getDomainFile() {
        return getOptional("input.domain.file");
    }

    public String getUrlFile() {
        return getOptional("input.url.file");
    }

    public double getSourceIplbs_hFileExtractFactor() {
        return Double.parseDouble(getOptional("input.source_address_iplbs_h.file.extract.factor"));
    }

    public double getSourceIplbs_lFileExtractFactor() {
        return Double.parseDouble(getOptional("input.source_address_iplbs_l.file.extract.factor"));
    }

    public int getSourceIplbs_hFileExtractCount() {
        return Integer.parseInt(getOptional("input.source_address_iplbs_h.file.extract.count"));
    }

    public int getSourceIplbs_lFileExtractCount() {
        return Integer.parseInt(getOptional("input.source_address_iplbs_l.file.extract.count"));
    }

    public double getDestinationIplbs_hFileExtractFactor() {
        return Double.parseDouble(getOptional("input.destination_address_iplbs_h.file.extract.factor"));
    }

    public double getDestinationIplbs_lFileExtractFactor() {
        return Double.parseDouble(getOptional("input.destination_address_iplbs_l.file.extract.factor"));
    }

    public int getDestinationIplbs_hFileExtractCount() {
        return Integer.parseInt(getOptional("input.destination_address_iplbs_h.file.extract.count"));
    }

    public int getDestinationIplbs_lFileExtractCount() {
        return Integer.parseInt(getOptional("input.destination_address_iplbs_l.file.extract.count"));
    }

    public double getDomain_hFileExtractFactor() {
        return Double.parseDouble(getOptional("input.domain_h.file.extract.factor"));
    }

    public double getDomain_lExtractFactor() {
        return Double.parseDouble(getOptional("input.domain_l.file.extract.factor"));
    }

    public int getDomain_hFileExtractCount() {
        return Integer.parseInt(getOptional("input.domain_h.file.extract.count"));
    }

    public int getDomain_lFileExtractCount() {
        return Integer.parseInt(getOptional("input.domain_l.file.extract.count"));
    }

    public double getUrl_hFileExtractFactor() {
        return Double.parseDouble(getOptional("input.url_h.file.extract.factor"));
    }

    public double getUrl_lFileExtractFactor() {
        return Double.parseDouble(getOptional("input.url_l.file.extract.factor"));
    }

    public int getUrl_hFileExtractCount() {
        return Integer.parseInt(getOptional("input.url_h.file.extract.count"));
    }

    public int getUrl_lFileExtractCount() {
        return Integer.parseInt(getOptional("input.url_l.file.extract.count"));
    }
}

package cn.edu.bistu.genDataset;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import org.apache.commons.io.IOUtils;

import cn.edu.bistu.genDataset.config.parameter;

public class GenerateDatasetConfigBase implements Serializable {

	private volatile Properties properties = getGenerateDatasetProperties();

	public static Properties getGenerateDatasetProperties() {
		File propFile = getGenerateDatasetPropertiesFile(parameter.GENERATE_DATASET_CONF_PROPERTIES_PATH);
		if (propFile == null || !propFile.exists()) {
			throw new RuntimeException("fail to locate " + parameter.GENERATE_DATASET_CONF_PROPERTIES_FILE);
		}
		Properties conf = new Properties();
		try {
			FileInputStream fileInputStream = new FileInputStream(propFile);
			conf.load(fileInputStream);
			IOUtils.closeQuietly(fileInputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return conf;
	}

	final protected String getOptional(String prop) {
		return getOptional(prop, null);
	}
	
	private static File getGenerateDatasetPropertiesFile(String path) {
		if (path == null) {
			return null;
		}
		return new File(path, parameter.GENERATE_DATASET_CONF_PROPERTIES_FILE);
	}

	protected String getOptional(String prop, String dft) {
		final String property = System.getProperty(prop);
		return property != null ? property : properties.getProperty(prop, dft);
	}

	public int getCount() {
		return Integer.parseInt(getOptional("generate.dataset.count"));
	}
	
	public int getSlices() {
		return Integer.parseInt(getOptional("generate.dataset.slices"));
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
		return doubleArray("generate.dataset.discontinuity.point");
    }

	public double[] getDistributions() {
        return doubleArray("generate.dataset.distribution");
	}
	
	public double getAccuracy() {
		return Double.parseDouble(getOptional("generate.dataset.accuracy"));
	}
	
	public String getOutputPath() {
		return getOptional("generate.dataset.output.path");
	}
	
	public String getOutputFile() {
		return getOptional("generate.dataset.output.file");
	}
	
	public String getSourceAddressIplbsFile_h() {
		return getOptional("generate.dataset.input.source_address_iplbs_h.file");
	}
	
	public String getSourceAddressIplbsFile_l() {
		return getOptional("generate.dataset.input.source_address_iplbs_l.file");
	}
	
	public double getSourceAddressIplbsFactor(){
		return Double.parseDouble(getOptional("generate.dataset.source_address_iplbs.factor"));
	}
	
	public String getDestinationAddressIplbsFile_h() {
		return getOptional("generate.dataset.input.destination_address_iplbs_h.file");
	}
	
	public String getDestinationAddressIplbsFile_l() {
		return getOptional("generate.dataset.input.destination_address_iplbs_l.file");
	}
	
	public double getDestinationAddressIplbsFactor(){
		return Double.parseDouble(getOptional("generate.dataset.destination_address_iplbs.factor"));
	}
	
	public String getDomainFile_h() {
		return getOptional("generate.dataset.input.domain_h.file");
	}
	
	public String getDomainFile_l() {
		return getOptional("generate.dataset.input.domain_l.file");
	}
	
	public double getDomainFactor(){
		return Double.parseDouble(getOptional("generate.dataset.domain.factor"));
	}
	
	public String getUrlFile_h() {
		return getOptional("generate.dataset.input.url_h.file");
	}
	
	public String getUrlFile_l() {
		return getOptional("generate.dataset.input.url_l.file");
	}
	
	public double getUrlFactor(){
		return Double.parseDouble(getOptional("generate.dataset.url.factor"));
	}
}

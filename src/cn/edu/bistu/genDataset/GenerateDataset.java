package cn.edu.bistu.genDataset;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import cn.edu.bistu.genDataset.config.*;

public class GenerateDataset {

	public static boolean generate(Long count, double[] discontinuityPoints, double[] distributions, double accuracy,
			String outputPath) {
		Path outputFile = Paths.get(outputPath, parameter.OUTPUT_FILE_NAME);
		BufferedWriter numberWriter = null;
		try {

			numberWriter = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
			while (0 != count--) {
				numberWriter.write(generateNumber(discontinuityPoints, distributions, accuracy));
				numberWriter.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (numberWriter != null) {
					numberWriter.close();
				}
			} catch (IOException e) {
				return false;
			}
		}
		return true;
	}

	public static String generateNumber(double[] discontinuityPoints, double[] distributions, double accuracy) {
		int digitalPoint = (int) (Math.random() * 100);
		int distributiontemp = 0;
		int distributionOffset = 0;
		for (int i = 0; i <= distributions.length; i++) {
			distributiontemp += distributions[i];
			if (digitalPoint < distributiontemp) {
				distributionOffset = i;
				break;
			}
		}
		double generateNumber = Math.random()
				* (discontinuityPoints[distributionOffset + 1] - discontinuityPoints[distributionOffset])
				+ discontinuityPoints[distributionOffset];
//		NumberFormat numberFormat = NumberFormat.getNumberInstance();
//		numberFormat.setMaximumFractionDigits((int) accuracy);
//		return numberFormat.format(generateNumber);
		DecimalFormat df = new DecimalFormat("#");
		return df.format(generateNumber);
	}

	public static void main(String[] args) {
		long time = System.currentTimeMillis();
		GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();
		long count = config.getCount();
		generate(count, config.getDiscontinuityPoints(),  config.getDistributions(), config.getAccuracy(), config.getOutputPath());
		System.out.println("生成"+(count/10000)+"万条数据, 累计耗时:"+(System.currentTimeMillis()-time)+"ms");
	}
}

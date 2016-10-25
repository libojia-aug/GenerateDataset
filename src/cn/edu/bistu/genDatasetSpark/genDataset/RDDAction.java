package cn.edu.bistu.genDatasetSpark.genDataset;

import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.List;

class RDDAction {
    private static SparkConf conf = new SparkConf().setAppName("generateDataset").setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf);
    private static GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();

    static JavaRDD<String> loadRDD(String fileAddress, int hFileExtractCount) {
        JavaRDD<String> inputDataset = sc.textFile(fileAddress);

        List<String> inputDataset_hList = inputDataset.takeOrdered(hFileExtractCount);

        JavaRDD<String> replaceRDD = inputDataset.subtract(sc.parallelize(inputDataset_hList))
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String lines) throws Exception {
                        int offset = parameter.INT_ZORE;
                        List<String> listTemp = new ArrayList<>();
                        if (Math.random() > (double) hFileExtractCount / offset) {
                            listTemp.add(lines);
                        }
                        return listTemp;
                    }
                });

        List<String> replace = replaceRDD.toArray();
        int replaceLength = replace.size();
        for (int i = 0; i < replaceLength; i++) {
            int offset = (int) Math.random() * replaceLength;
            inputDataset_hList.remove(offset);
            inputDataset_hList.add(offset, replace.get(i));
        }
        return sc.parallelize(inputDataset_hList);
    }

}

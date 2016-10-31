package cn.edu.bistu.genDatasetSpark.genDataset;

import cn.edu.bistu.genDataset.GenerateDatasetConfigBase;
import cn.edu.bistu.genDataset.config.parameter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.random.RandomRDDs;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class RDDAction {
    public static Logger logger = Logger.getLogger("action");
    static PairFunction<Tuple2<String, Tuple2<String, String>>, String, String> removebracket = new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> arg0) throws Exception {
            return new Tuple2<>(arg0._1, arg0._2._1 + parameter.SEPARATOR + arg0._2._2);
        }
    };
    private static SparkConf conf = new SparkConf().setAppName("generateDataset").setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf);
    private static GenerateDatasetConfigBase config = new GenerateDatasetConfigBase();
    private static int offset = parameter.INT_ZORE;

    static Broadcast<List<String>> loadBroadcast(String fileAddress, int fileExtractCount) {
        JavaRDD<String> inputDataset = sc.textFile(fileAddress);

        List<String> inputDataset_List = inputDataset.takeOrdered(fileExtractCount);

        JavaRDD<String> replaceRDD = inputDataset.subtract(sc.parallelize(inputDataset_List))
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String lines) throws Exception {
                        List<String> listTemp = new ArrayList<>();
                        if (Math.random() > (double) fileExtractCount / offset) {
                            listTemp.add(lines);
                        }
                        offset++;
                        return listTemp;
                    }
                });

        List<String> replace = replaceRDD.toArray();
        for (int i = 0; i < replace.size(); i++) {
            int offset = (int) (Math.random() * inputDataset_List.size());
            inputDataset_List.remove(offset);
            inputDataset_List.add(offset, replace.get(i));
        }
        logger.info(inputDataset_List);
        return sc.broadcast(inputDataset_List);
    }

    static JavaPairRDD<String, String> JavamergeData(Broadcast<List<String>> BList_h, Broadcast<List<String>> BList_l, int count_h,
                                                     int count, int slices) {
        return RandomRDDs.uniformJavaRDD(sc, count_h)
                .map(new Function<Double, String>() {
                    public String call(Double d) {
                        Double a = ((double) BList_h.value().size()) * d;
                        return BList_h.value().get(a.intValue());
                    }
                }).union(RandomRDDs.uniformJavaRDD(sc, count - count_h)
                        .map(new Function<Double, String>() {
                            public String call(Double d) {
                                Double a = ((double) BList_l.value().size()) * d;
                                return BList_l.value().get(a.intValue());
                            }
                        }))
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

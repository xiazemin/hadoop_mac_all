
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCount {
	@SuppressWarnings({"rawtypes","unchecked" })
	public static void main(String[] args) {
		final Pattern SPLIT = Pattern.compile(" ");

	SparkConf conf =new SparkConf().setMaster("local[1]").setAppName("word count");

	JavaSparkContext context = new JavaSparkContext(conf);

	JavaRDD<String> lines = context.textFile("/Users/wxzyhx/Downloads/paper1.txt");

	@SuppressWarnings("unchecked")
	JavaRDD words =lines.flatMap(new FlatMapFunction() {
		@Override
		public Iterator call(Object line)throws Exception {
          return (Iterator) Arrays.asList(SPLIT.split((String)line));
   }
});

	JavaPairRDD ones =words.mapToPair(new PairFunction() {
  @Override

	public Tuple2 call(Object word)throws Exception {

	return new Tuple2((String)word, 1);

	}
});

	JavaPairRDD counts =ones.reduceByKey(new Function2() {
@Override
public Integer call(Object arg0, Object arg1)throws Exception {

	return (Integer)arg0 + (Integer)arg1;

	}

	});

	List<Tuple2> output =counts.collect();

	for (Tuple2 tuple :output) {

	System.out.println(tuple._1() +": " +tuple._2());

	}

	context.close();

	}
}

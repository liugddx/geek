/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/17 16:10
 */
package invertedindex;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title InvertedIndex
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/17 16:10
 */
@Slf4j
public class InvertedIndex {
	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setAppName("InvertedIndex Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		JavaRDD<Tuple2<String,String>> unionRdd = sc.emptyRDD();

		FileSystem fileSystem = FileSystem.get(sc.hadoopConfiguration());
		RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path(args[0]), true);

		while (files.hasNext()){
			//文件路径
			Path path = new Path(files.next().getPath().toString());
			//文件名
			String name = path.getName();

			JavaRDD<String> words = sc.textFile(path.toString()).flatMap(s -> Arrays.asList(s.split(" ")).iterator());

			JavaRDD<Tuple2<String,String>> fileNameWord = words.map(s -> new Tuple2(name, s));

			unionRdd = unionRdd.union(fileNameWord);
		}

		JavaPairRDD<Tuple2<String, String>, Integer> word =
				unionRdd.mapToPair(one -> new Tuple2<>(one,1));


		//(文件名 单词) 词频
		JavaPairRDD<Tuple2<String, String>, Integer> fileWordCount = word.reduceByKey(Integer::sum);
		//单词 (文件名,词频)
		JavaPairRDD<String, String> wordFileCount = fileWordCount.mapToPair(one
				-> new Tuple2<>(one._1._2, String.format("(%s,%s)", one._1._1, one._2)));
		//单词 [(文件名,词频),(文件名,词频)]
		JavaPairRDD<String, String> list = wordFileCount.reduceByKey((s, s2) -> s + "," + s2);

		JavaRDD<Tuple2<String, String>> result = list.map(index -> new Tuple2<>(index._1, String.format("\"%s\",{%s}", index._1, index._2)));

		result.saveAsTextFile(args[1]);

		sc.close();
	}
}

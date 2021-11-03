package com.lisz.bigdata.spark2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountJava {
	public static void main(String[] args) {
		final SparkConf conf = new SparkConf();
		conf.setAppName("wc");
		conf.setMaster("local");

		final JavaSparkContext jsc = new JavaSparkContext(conf);
		final JavaRDD<String> fileRDD = jsc.textFile("data/testdata.txt");
		final JavaRDD<String> words = fileRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				final String[] arr = line.split("\\s+");
				return Arrays.asList(arr).iterator();
			}
		});
		final JavaPairRDD<String, Integer> pairWord = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		final JavaPairRDD<String, Integer> res = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer oldV, Integer v) throws Exception {
				return oldV + v;
			}
		});
		res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> value) throws Exception {
				System.out.println(value._1 + "\t" + value._2);
			}
		});
	}
}

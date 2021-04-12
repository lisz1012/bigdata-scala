package com.lisz.bigdata.spark;

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
		SparkConf conf = new SparkConf();
		conf.setAppName("java-word-count");
		conf.setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = jsc.textFile("data/testdata.txt");
		JavaRDD<String> words = fileRDD.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split("\\s+")).iterator();
			}
		});

		JavaPairRDD<String, Integer> pairWord = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		JavaPairRDD<String, Integer> res = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			public void call(Tuple2<String, Integer> value) throws Exception {
				System.out.println(value._1 + "\t" + value._2);
			}
		});
	}
}

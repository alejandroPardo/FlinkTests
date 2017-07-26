package com.pluralsight.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TopHashTag {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		InputStream input = null;

		try {
			String propFileName = "config.properties";
			InputStream in = TopHashTag.class.getClassLoader().getResourceAsStream(propFileName);

			if (in != null) {
				props.load(in);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			props.load(in);
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		
		env.addSource(new TwitterSource(props))
			.map(new MapToTweet())
			.flatMap(new FlatMapFunction<Tweet, Tuple2<String, Integer>>() {

				@Override
				public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> collector) throws Exception {
					for(String tag: value.getTags()) {
						collector.collect(new Tuple2<>(tag,1));
					}
				}
			})
			.keyBy(0)
			.timeWindow(Time.minutes(1))
			.sum(1)
			.timeWindowAll(Time.minutes(1))
			.apply(new AllWindowFunction<Tuple2<String,Integer>, Tuple3<Date, String, Integer>, TimeWindow>() {
				@Override
				public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<Date, String, Integer>> collector) throws Exception {
					String topTag = null;
					int count = 0;
					for(Tuple2<String, Integer> hashTag: iterable){
						if(hashTag.f1 > count){
							topTag = hashTag.f0;
							count = hashTag.f1;
						}
					}
					collector.collect(new Tuple3<>(new Date(timeWindow.getEnd()), topTag, count));
				}
			})
			.print();

		env.execute();
	}

	private static class MapToTweet implements MapFunction<String, Tweet> {

		static private final ObjectMapper mapper = new ObjectMapper();

		@Override
		public Tweet map(String tweetJsonStr) throws Exception {
			JsonNode tweetJson = mapper.readTree(tweetJsonStr);
			JsonNode textNode = tweetJson.get("text");
			JsonNode langNode = tweetJson.get("lang");
			List<String> tags = new ArrayList<String>();

			String text = textNode == null ? "" : textNode.textValue();
			String lang = langNode == null ? "" : langNode.textValue();
			JsonNode entities = tweetJson.get("entities");
			if(entities != null){
				JsonNode hashtags = entities.get("hashtags");
				Iterator<JsonNode> iter = hashtags.elements();
				while(iter.hasNext()){
					JsonNode node = iter.next();
					String hashtag = node.get("text").textValue();
					tags.add(hashtag);
				}
			}

			return new Tweet(lang, text, tags);
		}

	}
}

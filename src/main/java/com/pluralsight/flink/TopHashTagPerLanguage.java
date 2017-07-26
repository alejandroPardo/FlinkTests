package com.pluralsight.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TopHashTagPerLanguage {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		InputStream input = null;

		try {
			String propFileName = "config.properties";
			InputStream in = TopHashTagPerLanguage.class.getClassLoader().getResourceAsStream(propFileName);

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
			.flatMap(new FlatMapFunction<Tweet, Tuple3<String, String, Long>>() {
				@Override
				public void flatMap(Tweet value, Collector<Tuple3<String,String, Long>> collector) throws Exception {
					for(String tag: value.getTags()) {
						collector.collect(new Tuple3<>(value.getLanguage(),tag,1L));
					}
				}
				
			})
			.keyBy(0, 1)
			.timeWindow(Time.minutes(1))
			.reduce(new ReduceFunction<Tuple3<String,String,Long>>() {
				
				@Override
				public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
					return new Tuple3<>(value1.f0, value1.f1, value1.f2+value2.f2);
				}
			})
			.keyBy(0)
			.timeWindow(Time.minutes(1))
			.reduce(new ReduceFunction<Tuple3<String,String,Long>>() {
				@Override
				public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
					if(value1.f2 >= value2.f2){
						return value1;
					} else {
						return value2;
					}
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

package com.pluralsight.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NumberOfTweetsPerLanguage {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		InputStream input = null;

		try {
			String propFileName = "config.properties";
			InputStream in = NumberOfTweetsPerLanguage.class.getClassLoader().getResourceAsStream(propFileName);

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
			.keyBy(new KeySelector<Tweet, String>() {
				@Override
				public String getKey(Tweet tweet) throws Exception {
					return tweet.getLanguage();
				}
			})
			.timeWindow(Time.minutes(1))
			.apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {

				@Override
				public void apply(String language, 
						TimeWindow timeWindow, 
						Iterable<Tweet> iterable, 
						Collector<Tuple3<String, Long, Date>> collector) throws Exception {
					
					long count = 0;
					for(Tweet tweet: iterable){
						count++;
					}
					
					collector.collect(new Tuple3<>(language, count, new Date(timeWindow.getEnd())));
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

			String text = textNode == null ? "" : textNode.textValue();
			String lang = langNode == null ? "" : langNode.textValue();

			return new Tweet(lang, text);
		}

	}
}

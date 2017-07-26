package com.pluralsight.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FilterSpanishTweets {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		InputStream input = null;

		try {
			String propFileName = "config.properties";
			InputStream in = FilterSpanishTweets.class.getClassLoader().getResourceAsStream(propFileName);

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

		env.addSource(new TwitterSource(props)).map(new MapToTweet()).filter(new FilterFunction<Tweet>() {
			@Override
			public boolean filter(Tweet tweet) throws Exception {
				return tweet.getLanguage().equals("es");
			}
		}).print();

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


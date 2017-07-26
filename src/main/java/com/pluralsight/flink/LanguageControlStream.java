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
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LanguageControlStream {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		InputStream input = null;

		try {
			String propFileName = "config.properties";
			InputStream in = LanguageControlStream.class.getClassLoader().getResourceAsStream(propFileName);

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
		
		DataStream<LanguageConfig> controlStream = env.socketTextStream("localhost", 9876)
				.flatMap(new FlatMapFunction<String, LanguageConfig>() {

					@Override
					public void flatMap(String value, Collector<LanguageConfig> collector) throws Exception {
						for(String langConfig: value.split(",")){
							String[] kvPair = langConfig.split("=");
							collector.collect(new LanguageConfig(kvPair[0], Boolean.parseBoolean(kvPair[1])));
						}
					}
				});
		
		env.addSource(new TwitterSource(props))
			.map(new MapToTweet())
			.keyBy(new KeySelector<Tweet, String>() {
				@Override
				public String getKey(Tweet value) throws Exception {
					return value.getLanguage();
				}
			})
			.connect(
					controlStream
					.keyBy(new KeySelector<LanguageConfig, String>(){

						@Override
						public String getKey(LanguageConfig value) throws Exception {
							return value.getLanguage();
						}
					}))
			.flatMap(new RichCoFlatMapFunction<Tweet, LanguageConfig, Tuple2<String, String>>( ) {
				ValueStateDescriptor<Boolean> shouldProcess = new ValueStateDescriptor<Boolean>("languageConfig", Boolean.class); 
				
				@Override
				public void flatMap1(Tweet value, Collector<Tuple2<String, String>> out) throws Exception {
					Boolean processLanguage = getRuntimeContext().getState(shouldProcess).value();
					if(processLanguage != null && processLanguage){
						for(String tag: value.getTags()){
							out.collect(new Tuple2<>(value.getLanguage(), tag));
						}
					}
				}

				@Override
				public void flatMap2(LanguageConfig value, Collector<Tuple2<String, String>> out) throws Exception {
					getRuntimeContext().getState(shouldProcess).update(value.isShouldProcess());
					
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
	
	
	static class LanguageConfig {
		private String language;
		private boolean shouldProcess;

		/**
		 * @param language
		 * @param shouldProcess
		 */
		public LanguageConfig(String language, boolean shouldProcess) {
			super();
			this.language = language;
			this.shouldProcess = shouldProcess;
		}

		/**
		 * @return the language
		 */
		public String getLanguage() {
			return language;
		}

		/**
		 * @param language the language to set
		 */
		public void setLanguage(String language) {
			this.language = language;
		}

		/**
		 * @return the shouldProcess
		 */
		public boolean isShouldProcess() {
			return shouldProcess;
		}

		/**
		 * @param shouldProcess the shouldProcess to set
		 */
		public void setShouldProcess(boolean shouldProcess) {
			this.shouldProcess = shouldProcess;
		}

	}
}

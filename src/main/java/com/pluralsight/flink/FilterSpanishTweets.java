package com.pluralsight.flink;

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
        props.setProperty(TwitterSource.CONSUMER_KEY, "yqNAHs4kvLAGx5JeOmt6A");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "c9zAhbP74VrLOV5MKxK8AfPL4oQfb9lieAiA8OyRQA");
        props.setProperty(TwitterSource.TOKEN, "82975191-MldWNStfYekrHBVKcxGj3jIFRL94saJXkLGegJ9JT");
        props.setProperty(TwitterSource.TOKEN_SECRET, "6JDYHwm4Fwuyb0zcaNlAoq4QDrjQkfqtgHdl3LNq9MIXe");
        
        env.addSource(new TwitterSource(props))
        	.map(new MapToTweet())
        	.filter(new FilterFunction<Tweet>() {
        		@Override
        		public boolean filter(Tweet tweet) throws Exception {
        			return tweet.getLanguage().equals("es");
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

class Tweet {
    private String language;
    private String text;
	/**
	 * @param language
	 * @param text
	 */
	public Tweet(String language, String text) {
		super();
		this.language = language;
		this.text = text;
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
	 * @return the text
	 */
	public String getText() {
		return text;
	}
	/**
	 * @param text the text to set
	 */
	public void setText(String text) {
		this.text = text;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Tweet [language=" + language + ", text=" + text + "]";
	}
    
    
}
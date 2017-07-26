package com.pluralsight.flink;

import java.util.List;

/**
 * The Class Tweet.
 */
public class Tweet {

	/** The language. */
	private String language;

	/** The text. */
	private String text;

	/** The tags. */
	private List<String> tags;

	/**
	 * Instantiates a new tweet.
	 *
	 * @param language the language
	 * @param text the text
	 */
	public Tweet(String language, String text) {
		super();
		this.language = language;
		this.text = text;
	}

	/**
	 * Instantiates a new tweet.
	 *
	 * @param language the language
	 * @param text the text
	 * @param tags the tags
	 */
	public Tweet(String language, String text, List<String> tags) {
		super();
		this.language = language;
		this.text = text;
		this.tags = tags;
	}

	/**
	 * Gets the language.
	 *
	 * @return the language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * Sets the language.
	 *
	 * @param language the language to set
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * Gets the text.
	 *
	 * @return the text
	 */
	public String getText() {
		return text;
	}

	/**
	 * Sets the text.
	 *
	 * @param text the text to set
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * Gets the tags.
	 *
	 * @return the tags
	 */
	public List<String> getTags() {
		return tags;
	}

	/**
	 * Sets the tags.
	 *
	 * @param tags the tags to set
	 */
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Tweet [language=" + language + ", text=" + text + ", tags=" + tags + "]";
	}

}
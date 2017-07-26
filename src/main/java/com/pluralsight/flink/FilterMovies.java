package com.pluralsight.flink;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class FilterMovies {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String input = parameters.getRequired("input");
        String output = parameters.getRequired("output");
        
        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile(input)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        lines.map(new MapFunction<Tuple3<Long,String,String>, Movie>(){
            @Override
            public Movie map(Tuple3<Long,String,String> csvLine) throws Exception {
                String movieName = csvLine.f1;
                String[] genres = csvLine.f2.split("\\|");
                return new Movie(movieName, new HashSet<>(Arrays.asList(genres)));
            }

        })
        .filter(new FilterFunction<Movie>(){
        	@Override
        	public boolean filter(Movie movie) throws Exception {
        		return movie.getGenres().contains("Drama");
        	}
        })
        .writeAsText(output);
        env.execute();
    }
}

class Movie {
    private String name;
    private Set<String> genres;

    public Movie(String name, Set<String> genres) {
        this.name = name;
        this.genres = genres;
    }

    public String getName() {
        return name;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setGenres(Set<String> genres) {
        this.genres = genres;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "name='" + name + '\'' +
                ", genres=" + genres +
                '}';
    }
}
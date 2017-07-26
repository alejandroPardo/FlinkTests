package com.pluralsight.flink;

import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class Top10Movies {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(true,true,false)
                .types(Long.class, String.class);

        DataSet<Tuple2<Long, Double>> sorted = env.readCsvFile("ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class)
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                	@Override
                	public void reduce(Iterable<Tuple2<Long, Double>> iterable, Collector <Tuple2<Long,Double>> collector) throws Exception {
                		Long movieId = null;
                		double total = 0;
                		int count = 0;
                		for(Tuple2<Long,Double> value: iterable){
                			movieId = value.f0;
                			total += value.f1;
                			count++;
                		}
                		
                		if(count > 50) {
                			collector.collect(new Tuple2<>(movieId, total / count));
                		}
                	}
                })
                .partitionCustom(new Partitioner<Double>() {
                	@Override
                	public int partition(Double key, int numPartition){
                		return key.intValue() -1;
                	}
                }, 1)
                .setParallelism(5)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<Long,Double>, Tuple2<Long,Double>>() {
                	@Override
                	public void mapPartition(Iterable<Tuple2<Long,Double>> iterable, Collector<Tuple2<Long,Double>> collector) throws Exception {
                		Iterator<Tuple2<Long,Double>> iter = iterable.iterator();
                		for(int i=0; i<10 && iter.hasNext(); i++) {
                			collector.collect(iter.next());
                		}
                	}
                })
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple2<Long,Double>, Tuple2<Long,Double>>() {
                	@Override
                	public void mapPartition(Iterable<Tuple2<Long,Double>> iterable, Collector<Tuple2<Long,Double>> collector) throws Exception {
                		Iterator<Tuple2<Long,Double>> iter = iterable.iterator();
                		for(int i=0; i<10 && iter.hasNext(); i++) {
                			collector.collect(iter.next());
                		}
                	}
                });
        
        movies.join(sorted)
        	.where(0)
        	.equalTo(0)
        	.with(new JoinFunction<Tuple2<Long,String>, Tuple2<Long,Double>, Tuple3<Long,String,Double>>() {
        		@Override
        		public Tuple3<Long,String,Double> join(Tuple2<Long,String> movie, Tuple2<Long,Double> rating) throws Exception {
        			return new Tuple3<>(movie.f0, movie.f1, rating.f1);
        		}
        	})
        	.print();
        

    }
}

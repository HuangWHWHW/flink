package org.apache.flink.streaming.examples.accumulator;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Created by h00292103 on 2015/9/16.
 */
public class IntCount {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Integer> source = env.addSource(new SourceFunction<Integer>() {
			private boolean running = true;

			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				while (running) {
					ctx.collect(123);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		DataStream<Integer> temp1 = source.flatMap(new Accumulator());
		temp1.print();

		try{
			env.execute();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static class Accumulator extends RichFlatMapFunction<Integer, Integer> {

		private IntCounter count = new IntCounter();

		@Override
		public void open(Configuration parameters){
			getRuntimeContext().addAccumulator("count", this.count);
		}

		@Override
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			this.count.add(1);
			out.collect(value + this.count.getLocalValue());
			Thread.sleep(500);
		}

		@Override
		public void close() throws Exception {

		}
	}

}

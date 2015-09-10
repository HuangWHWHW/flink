package org.apache.flink.streaming.examples.SmartPCC;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by h00292103 on 2015/7/22.
 */
public class SmartPccPoc {

	public static Logger LOG = LoggerFactory.getLogger(SmartPccPoc.class);

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(1);

		// get input data
		DataStream<String> edr_source = env.socketTextStream(hostName, port_edr, '\n', 0);
		DataStream<String> xdr_source = env.socketTextStream(hostName, port_xdr, '\n', 0);

		/* 通道引流s_edr，规则：
		 * create stream s_edr(TriggerType uint64, MSISDN string, QuotaName string,
		 * QuotaConsumption uint32, QuotaBalance uint32, CaseID uint32, bbb uint32)
		 * as select * from tcp_channel_edr.edr_event partition MSISDN;
         */
		DataStream<Tuple6<Long, String, String, Integer, Integer, Integer>> s_edr =
				edr_source.map(new MapFunction<String, Tuple6<Long, String, String, Integer, Integer, Integer>>() {
					@Override
					public Tuple6<Long, String, String, Integer, Integer, Integer> map(String value) throws Exception {
						// normalize and split the line
						String[] tokens = value.split("\\|");

						Tuple6<Long, String, String, Integer, Integer, Integer> out = new Tuple6<Long, String, String, Integer, Integer, Integer>
								(Long.valueOf(tokens[0]), tokens[1], tokens[2], Integer.valueOf(tokens[3]), Integer.valueOf(tokens[4]), Integer.valueOf(tokens[5]));

						return out;
					}
				}).partitionByHash(0);

		/* 通道引流s_xdr，规则：
		 * create stream s_xdr(aaa uint64, MSISDN string, HOST string, CaseID uint32)
		 * as select * from tcp_channel_xdr.xdr_event partition MSISDN;
		 */
		DataStream<Tuple3<String, String, Integer>> s_xdr =
				xdr_source.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
					@Override
					public Tuple3<String, String, Integer> map(String value) throws Exception {
						// normalize and split the line
						String[] tokens = value.split("\\|");

						Tuple3<String, String, Integer> out = new Tuple3<String, String, Integer>
								(tokens[0], tokens[1], Integer.valueOf(tokens[2]));

						return out;
					}
				}).partitionByHash(0);

		DataStream<Tuple7<Long, String, String, Integer, Integer, Integer, Integer>> temp1
				= s_edr.flatMap(new FlatMapFunction<Tuple6<Long, String, String, Integer, Integer, Integer>, Tuple7<Long, String, String, Integer, Integer, Integer, Integer>>() {
			@Override
			public void flatMap(Tuple6<Long, String, String, Integer, Integer, Integer> in, Collector<Tuple7<Long, String, String, Integer, Integer, Integer, Integer>> out) throws Exception {
				if (in.f2.equals("GPRS") && (in.f3 * 10 >= in.f4 * 9)){
					out.collect(new Tuple7<Long, String, String, Integer, Integer, Integer, Integer>(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, 1));
				}
				else if (in.f2.equals("GPRS") && in.f4 == 0){
					out.collect(new Tuple7<Long, String, String, Integer, Integer, Integer, Integer>(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, 2));
				}
				else if (in.f2.equals("FREE") && in.f4 == 0) {
					out.collect(new Tuple7<Long, String, String, Integer, Integer, Integer, Integer>(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, 3));
				}
			}
		}).partitionByHash(1);

		DataStream<Tuple7<Long, String, String, Integer, Integer, Integer, Integer>> temp_result1
				= temp1.join(s_xdr)
				.onWindow(1, TimeUnit.MINUTES)
				.where(1)
				.equalTo(0)
				.with(new JoinFunction<Tuple7<Long, String, String, Integer, Integer, Integer, Integer>, Tuple3<String, String, Integer>, Tuple7<Long, String, String, Integer, Integer, Integer, Integer>>() {
					@Override
					public Tuple7<Long, String, String, Integer, Integer, Integer, Integer> join(Tuple7<Long, String, String, Integer, Integer, Integer, Integer> first, Tuple3<String, String, Integer> second) throws Exception {
						Tuple7<Long, String, String, Integer, Integer, Integer, Integer> out
								= first.copy();
						return out;
					}
				}).partitionByHash(1);

		if (fileOutput) {
			temp_result1.writeAsText(outputPath, 1);
		} else {
			temp1.print();
			temp_result1.print();
		}

		// execute program
		env.execute("WordCount from SocketTextStream Example");
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String hostName;
	private static int port_edr;
	private static int port_xdr;
	private static String outputPath;
	private static Long Receive = 0L;
	private static Long count = 1000000L;
	private static Long start = -1L;

	private static boolean parseParameters(String[] args) {

		// parse input arguments
		if (args.length == 3) {
			hostName = args[0];
			port_edr = Integer.valueOf(args[1]);
			port_xdr = Integer.valueOf(args[2]);
		}
		else if (args.length == 4) {
			fileOutput = true;
			hostName = args[0];
			port_edr = Integer.valueOf(args[1]);
			port_xdr = Integer.valueOf(args[2]);
			outputPath = args[3];
		}else {
			System.err.println("Usage: SmartPccPoc <hostname> <port> [<output path>]");
			return false;
		}
		return true;
	}
}

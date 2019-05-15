package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 指标聚合
 * @author xushuanglu
 *
 */
public class EsMaxAggregationTest {

	public final static String HOST = "127.0.0.1";

	public final static int PORT = 9300; // http请求的端口是9200，客户端是9300

	private static TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void getConnect() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
	}
	
	public static void main(String[] args) {
		//1、使用MaxAggregationBuilder创建一个求最大值的聚合查询，聚合字段为price
		MaxAggregationBuilder aggregationBuilder = AggregationBuilders.max("agg")
				.field("price");
		//2、设置要搜索的索引名称
		SearchResponse response = client.prepareSearch("books")
				.addAggregation(aggregationBuilder)//3、调用addAggregation方法
				.get();
		//4、通过SearchResponse对象返回聚合结果
		Max agg = response.getAggregations().get("agg");
		//5、获取最终聚合结果
		double value = agg.getValue();
		System.out.println(value);
	}
	//指标聚合
	@Test
	public void testMax() {
		//1、使用MaxAggregationBuilder创建一个求最大值的聚合查询，聚合字段为price
		MaxAggregationBuilder aggregationBuilder = AggregationBuilders.max("agg")
				.field("price");
		//2、设置要搜索的索引名称
		SearchResponse response = client.prepareSearch("books")
				.addAggregation(aggregationBuilder)//3、调用addAggregation方法
				.get();
		//4、通过SearchResponse对象返回聚合结果
		Max agg = response.getAggregations().get("agg");
		//5、获取最终聚合结果
		double value = agg.getValue();
		System.out.println(value);
	}
	
	@Test
	public void testMin() {
		//1、使用MaxAggregationBuilder创建一个求最大值的聚合查询，聚合字段为price
		MinAggregationBuilder aggregationBuilder = AggregationBuilders.min("agg")
				.field("price");
		//2、设置要搜索的索引名称
		SearchResponse response = client.prepareSearch("books")
				.addAggregation(aggregationBuilder)//3、调用addAggregation方法
				.execute()
				.actionGet();
		//4、通过SearchResponse对象返回聚合结果
		Min agg = response.getAggregations().get("agg");
		//5、获取最终聚合结果
		double value = agg.getValue();
		System.out.println(value);
	}

}

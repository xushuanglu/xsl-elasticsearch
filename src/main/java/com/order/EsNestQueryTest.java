package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 嵌套查询
 * @author xushuanglu
 *
 */
public class EsNestQueryTest {

	public final static String HOST = "127.0.0.1";

	public final static int PORT = 9300; // http请求的端口是9200，客户端是9300

	private static TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void getConnect() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
	}
	
	@Test
	public void testNestedQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.nestedQuery("obj1", QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("obj1.name", "blue"))
						.must(QueryBuilders.rangeQuery("obj1.name").gt(5)), ScoreMode.Avg)).get();
	
		// 2 打印查询结果
		SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
		System.out.println("查询结果有：" + hits.getTotalHits() + "条");
		
		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()) {
			SearchHit searchHit = iterator.next(); // 每个查询对象
			System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
		}
		// 3 关闭连接
		client.close();
		System.out.println(searchResponse.status());
	}
	
	@Test
	public void testHasChildQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.nestedQuery("obj1", QueryBuilders.boolQuery()
						.must(QueryBuilders.matchQuery("obj1.name", "blue"))
						.must(QueryBuilders.rangeQuery("obj1.name").gt(5)), ScoreMode.Avg)).get();
		
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("create_time").gte("2019-05-15");
//		QueryBuilder hasChildQuery = QueryBuilders.hasChildQuery("employee",rangeQuery);
		
		// 3 关闭连接
		client.close();
	}
}

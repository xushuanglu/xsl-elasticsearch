package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 复合查询
 * @author xushuanglu
 *
 */
public class EsFuHeQueryTest {

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
	public void testConstantScoreQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("message", "study")).boost(2.0f)).get();
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
	public void testDisMaxQuery() {
		SearchRequestBuilder searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.disMaxQuery()
						.add(QueryBuilders.termQuery("message", "study"))
						.add(QueryBuilders.termQuery("message", "elastrcsearch"))
						.boost(1.2f)
						.tieBreaker(0.7f));
		// 3 关闭连接
		client.close();
	}
	
	@Test
	public void testBoolQuery() {
		
		MatchQueryBuilder matchQuery1 = QueryBuilders.matchQuery("message", "study");
		MatchQueryBuilder matchQuery2 = QueryBuilders.matchQuery("user", "xiaolu001");
		
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("total_cost").gte(70);
		
		QueryBuilder boolQuery = QueryBuilders.boolQuery()
				.must(matchQuery1)//必须包含
				.should(matchQuery2)//可以包含也可以不包含
				.mustNot(rangeQuery);//价格不高于70
		// 3 关闭连接
		client.close();
	}
	
	@Test
	public void testIndecisQuery() {
		
		MatchQueryBuilder matchQuery1 = QueryBuilders.matchQuery("message", "study");
		MatchQueryBuilder matchQuery2 = QueryBuilders.matchQuery("user", "xiaolu001");
		
		// 3 关闭连接
		client.close();
	}
	
	@Test
	public void testBoostingQuery() {
		
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("message", "study");
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("create_time").lte("2019-05-15");
		
		QueryBuilder boostingQuery =QueryBuilders.boostingQuery(matchQuery, rangeQuery).negativeBoost(0.2f);
		
		// 3 关闭连接
		client.close();
	}
	
	
}

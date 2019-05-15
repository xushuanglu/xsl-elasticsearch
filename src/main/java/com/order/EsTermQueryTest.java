package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 词项查询
 * @author xushuanglu
 *
 */
public class EsTermQueryTest {

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
	public void testTermQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.termQuery("user", "xiaolu001")).get();
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
	public void testTermsQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.termsQuery("user", "xiaolu001","xiaolu002")).get();
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
	public void testRangeQuery() {
		SearchResponse searchResponse = client.prepareSearch("order").setTypes("doc")
				.setQuery(QueryBuilders.rangeQuery("total_cost").from(2200).to(2280).includeLower(true).includeUpper(true)).get();
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
	public void testSearch() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.existsQuery("message")).get();
		
		SearchResponse searchResponse1 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.prefixQuery("meaasge", "stu")).get();
		
		SearchResponse searchResponse2 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.wildcardQuery("meaasge", "stu?")).get();
		
		SearchResponse searchResponse3 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.wildcardQuery("meaasge", "stu?")).get();
		
		SearchResponse searchResponse4 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.regexpQuery("meaasge", "stu.*")).get();
		
		SearchResponse searchResponse5 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.fuzzyQuery("meaasge", "study")).get();
		
		SearchResponse searchResponse6 = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.typeQuery("study")).get();
		
//		SearchResponse searchResponse7 = client.prepareSearch("twitter").setTypes("tweet")
//				.setQuery(QueryBuilders.idsQuery().ids("1","2"));
		
	}
}

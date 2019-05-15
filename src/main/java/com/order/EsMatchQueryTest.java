package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 全文查询
 * @author xushuanglu
 *
 */
public class EsMatchQueryTest {
	
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
		QueryBuilder matchQuery = QueryBuilders.matchQuery("title", "2")
				.operator(Operator.AND);
		HighlightBuilder highlightBuilder = new HighlightBuilder()
				.field("title")
				.preTags("<span style=\"color:red\">")
				.preTags("</span>");
		
		SearchResponse response = client.prepareSearch("film")
				.setQuery(matchQuery)
				.highlighter(highlightBuilder)
				.setSize(100)
				.get();
		System.out.println(response.status());
		
		SearchHits hits = response.getHits();
		System.out.println("共搜到：" + hits.getTotalHits() + "条数据");
		
		for(SearchHit hit : hits) {
			System.out.println("Source:" + hit.getSourceAsString());
			System.out.println("Source as Map:" + hit.getSourceAsMap());
			System.out.println("Index:" + hit.getIndex());
			System.out.println("Type:" + hit.getType());
			System.out.println("Id:" + hit.getId());
//			System.out.println("Price:" + hit.getSource().get("price"));
			System.out.println("Score:" + hit.getScore());
			
			Text[] text = hit.getHighlightFields().get("title").getFragments();
			if(text != null) {
				for(Text str : text) {
					System.out.println(str.string());
				}
			}
		}
	}
	
	
	@Test
	public void testMatchAllQuery() {
		SearchResponse searchResponse = client.prepareSearch("film").setTypes("dongzuo")
				.setQuery(QueryBuilders.matchAllQuery()).get();

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
	}
	
	@Test
	public void testMatchParaseQuery() {
		SearchResponse searchResponse = client.prepareSearch("film").setTypes("dongzuo")
				.setQuery(QueryBuilders.matchPhraseQuery("title", "2")).get();
		
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
	}
	
	@Test
	public void testMatchParasePrefixQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.matchPhrasePrefixQuery("message", "study e")).get();
		
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
	}
	
	@Test
	public void testMultiMatchQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.multiMatchQuery("study", "message","user")).get();
		
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
	}
	
	@Test
	public void testCommonTermsQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.commonTermsQuery("message", "study")).get();
		
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
	}
	
	@Test
	public void testQueryStringQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.queryStringQuery("+study -haha")).get();
		
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
	}
	
	@Test
	public void testSimpleQueryStringQuery() {
		SearchResponse searchResponse = client.prepareSearch("twitter").setTypes("tweet")
				.setQuery(QueryBuilders.simpleQueryStringQuery("+study -haha")).get();
		
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
	}
	
	
}

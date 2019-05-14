package com.order;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.mr.ElasticsearchTest2;
import com.mr.EsClientUtils;

public class OrderEs {

	private Logger logger = LoggerFactory.getLogger(ElasticsearchTest2.class);

	public final static String HOST = "127.0.0.1";

	public final static int PORT = 9300; // http请求的端口是9200，客户端是9300

	private TransportClient client = null;
	
	@Autowired
	EsClientUtils esClientUtils;

	/**
	 * 获取客户端连接信息
	 * 
	 * @Title: getConnect
	 * @author sunt
	 * @date 2017年11月23日
	 * @return void
	 * @throws UnknownHostException
	 */
	@SuppressWarnings({ "resource"})
	@Before
	public void getConnect() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
		logger.info("连接信息:" + client.toString());
	}

	/**
	 * 关闭连接
	 * 
	 * @Title: closeConnect
	 * @author sunt
	 * @date 2017年11月23日
	 * @return void
	 */
	@After
	public void closeConnect() {
		if (null != client) {
			logger.info("执行关闭连接操作...");
			client.close();
		}
	}
	
	/**
	 * 查询订单所有数据
	 * @throws Exception
	 */
	@Test
	public void searchOrderBy()throws Exception{
	    SearchRequestBuilder srb=client.prepareSearch("order").setTypes("doc");
	    SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
	            .addSort("id", SortOrder.DESC)
	            .execute()
	            .actionGet(); // 分页排序所有
	    SearchHits hits=sr.getHits();
	    for(SearchHit hit:hits){
	        System.out.println(hit.getSourceAsString());
	    }
	}
	
	/**
	 * 查询订单所有数据
	 * @throws Exception
	 */
	@Test
	public void searchOrderByTerm()throws Exception{
		SearchRequestBuilder srb=client.prepareSearch("order").setTypes("doc");
		SearchResponse sr=srb.setQuery(QueryBuilders.termQuery("order_title", 20))
				.addSort("id", SortOrder.DESC)
				.execute()
				.actionGet(); // 分页排序所有
		SearchHits hits=sr.getHits();
		for(SearchHit hit:hits){
			System.out.println(hit.getSourceAsString());
		}
	}
	
}

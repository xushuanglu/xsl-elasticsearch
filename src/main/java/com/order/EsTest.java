package com.order;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
/**
 * Elasticsearch JAVA API学习
 * @author xushuanglu
 *
 */
public class EsTest {
	
	public final static String HOST = "127.0.0.1";

	public final static int PORT = 9300; // http请求的端口是9200，客户端是9300

	private static TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void getConnect() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
	}
	
	// 判断索引是否存在
	@Test
	public  void testIndexExists() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		IndicesExistsResponse existsResponse = indicesAdminClient
				.prepareExists("order")//索引名称
				.get();
		System.out.println(existsResponse.isExists());
	}
	// 判断类型是否存在
	@Test
	public  void testTypeExists() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		TypesExistsResponse existsResponse = indicesAdminClient
				.prepareTypesExists("order")//索引名称
				.setTypes("doc")
				.get();
		System.out.println(existsResponse.isExists());
	}
	//创建一个索引：索引名称必需小写，否则会报错
	@Test
	public  void testCreateIndex() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		CreateIndexResponse cResponse = indicesAdminClient.prepareCreate("es").get();
		System.out.println(cResponse.isAcknowledged());
	}
	//创建索引并设置Settings
	@Test
	public  void testCreateIndexAndSettings() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		CreateIndexResponse cResponse = indicesAdminClient
				.prepareCreate("es1")
				.setSettings(Settings.builder()
						.put("index.number_of_shards",3)
						.put("index.number_of_replicas",2)
						)
				.get();
		System.out.println(cResponse.isAcknowledged());
	}
	//更新副本
	@Test
	public  void testUpdateSettings() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		UpdateSettingsResponse upResponse = indicesAdminClient
				.prepareUpdateSettings("es1")
				.setSettings(Settings.builder().put("index.number_of_replicas",0))
				.get();
		System.out.println(upResponse.isAcknowledged());		
	}
	//删除索引
	@Test
	public  void testDeleteIndex() {
		IndicesAdminClient indicesAdminClient = client.admin().indices();
		DeleteIndexResponse deleteIndexResponse = indicesAdminClient
				.prepareDelete("es1").get();
		System.out.println(deleteIndexResponse.isAcknowledged());		
	}
	
	//创建文档（使用Map）
	@Test
	public void createDoc() {
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("user", "xiaolu");
		map.put("postDate", "2019-05-14");
		map.put("message", "study elasticsearch");
		IndexResponse indexResponse = client
				.prepareIndex("twitter", "tweet","2")
				.setSource(map)
				.get();
		System.out.println(indexResponse.status());
	}
	
	//创建文档（使用Elasticsearch帮助类
	@Test
	public void createDoc1() throws IOException {
		XContentBuilder doc = new XContentFactory().jsonBuilder().startObject()
				.field("user", "xiaolu002")
				.field("postDate", "2019-05-14")
				.field("message", "study elasticsearch")
				.endObject();
		System.out.println(doc.toString());
		IndexResponse indexResponse = client
				.prepareIndex("twitter", "tweet","4")
				.setSource(doc)
				.get();
		System.out.println(indexResponse.status());
	}
	
	// 获取文档
	@Test
	public void testGetDoc() throws IOException {
		GetResponse getResponse = client.prepareGet("twitter", "tweet", "2").get();
		System.out.println(getResponse.getSourceAsString());
	}
	
	// 删除文档
	@Test
	public void testDeleteDoc() throws IOException {
		DeleteResponse delResponse = client.prepareDelete("twitter", "tweet", "3").get();
		System.out.println(delResponse.status());
	}
	
	// 更新文档
	@Test
	public void testUpdateDoc() throws IOException, InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("tweet");
		updateRequest.id("2");
		updateRequest.doc(new XContentFactory().jsonBuilder().startObject().field("message","haha").endObject());
		client.update(updateRequest).get();
	}
	
	// 查询删除
	@Test
	public void testSearchAndDelete() throws IOException, InterruptedException, ExecutionException {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)//传入TransportClient对象
				.filter(QueryBuilders.matchQuery("message","haha"))//传入删除的Query
				.source("twitter")//设置索引名称
				.get();
		long deleted = response.getDeleted();//被删除文档的数目
		System.out.println(deleted);
	}
	
	// 批量获取
	@Test
	public void testMultiGet() throws IOException, InterruptedException, ExecutionException {
		MultiGetResponse multiGetResponse = client.prepareMultiGet()
				.add("twitter", "tweet","1")//通过单一id获取一个文档
				.add("twitter", "tweet","2","3","4")//传入多个id，从相同的索引名/类型名中获取多个文档
				.add("film","dongzuo","o8tHm2oBwV7xDiMSs5HV")//可以同时获取不同索引中的文档
				.get();
		for (MultiGetItemResponse itemResponse : multiGetResponse) {//遍历结果集
			GetResponse getResponse = itemResponse.getResponse();
			if(getResponse != null && getResponse.isExists()) {//检查文档是否存在
				String json = getResponse.getSourceAsString();//获取源文件
				System.out.println(json);
			}
		}
	}
	
}

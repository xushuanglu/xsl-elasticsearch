package com.mr;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsClientUtils {

	private Logger logger = LoggerFactory.getLogger(EsClientUtils.class);

	public final static String HOST = "127.0.0.1";//本地

	public final static int PORT = 9300;// http请求的端口是9200，客户端是9300

	/**
	 * 测试Elasticsearch客户端连接
	 * 
	 * @Title: testES
	 * @author xushuanglu
	 * @date 2017年11月22日
	 * @return void
	 * @throws UnknownHostException
	 */
	@Test
	public void testES() throws UnknownHostException {
		// 创建客户端
		@SuppressWarnings("resource")
		TransportClient client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddresses(
				new TransportAddress(InetAddress.getByName(HOST), PORT));

		logger.debug("Elasticsearch connect info:" + client.toString());
		// 关闭客户端
		client.close();
	}

	public static String getHost() {
		return HOST;
	}

	public static int getPort() {
		return PORT;
	}
	
	
}

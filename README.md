Elasticsearch学习

org.elasticsearch.client.transport.TransportClient
使用TransportClient创建的client对象可以通过传输模块远程与Elasticsearch集群建立连接。通过轮询调度的方式和服务器交互。
创建TransportClient：
**********************************************************************************
public final static String HOST = "127.0.0.1";

	public final static int PORT = 9300; // http请求的端口是9200，客户端是9300

	private TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void getConnect() throws UnknownHostException {
		client = new PreBuiltTransportClient(Settings.EMPTY)
				.addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
	}
**********************************************************************************
Settings对象中可以添加配置信息。如果在配置文件中设置的Elasticsearch集群名称不是默认的elasticsearch，就需要在Settings对象中指定集群的名称：
**********************************************************************************
Settings settings= Settings.builder()
	.put("cluster.name","myClusterName")
	.build();
TransportClient client = PreBuiltTransportClient(settings);
**********************************************************************************
TransportClient对象自带集群探测功能，可以自动添加新的主机，自动移除旧的主机。如果想要打开集群探测功能，需要设置client.transport.sniff的属性为true。
**********************************************************************************
Settings settings= Settings.builder()
	.put("client.transport.sniff",true)
	.build();
TransportClient client = PreBuiltTransportClient(settings);
**********************************************************************************
更多TransportClient的配置：
	client.transport.ignore_cluster_name:设置为true会忽略节点的集群名称验证。
	client.transport.ping_timeout:设置ping命令的响应时间，默认5秒
	client.transport.nodes_sampler_interval:设置检查节点可用性的频率，默认是5秒。

##索引管理
获取IndicesAdminClient indicesAdminClient  = client.admin().indices();

1、QueryBuilders查询创建类
2、Settings添加配置信息
3、IndicesAdminClient操作方法类(新增索引，更新索引，删除索引等等)


#文档给管理

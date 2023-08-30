package com.epochong.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author chongwang11
 * @date 2023-02-02 17:42
 * @description
 */
@Slf4j
public class EsOpTest {

    //es操作客户端
    private static RestHighLevelClient restHighLevelClient;
    //批量操作的对象
    private static BulkProcessor bulkProcessor;

    static {
        List<HttpHost> httpHosts = new ArrayList<>();
        //填充数据
        httpHosts.add(new HttpHost("127.0.0.1", 9200));
        //填充host节点
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));

        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(1000);
            requestConfigBuilder.setSocketTimeout(1000);
            requestConfigBuilder.setConnectionRequestTimeout(1000);
            return requestConfigBuilder;
        });

        //填充用户名密码
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("userName", "password"));

        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(30);
            httpClientBuilder.setMaxConnPerRoute(30);
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });

        restHighLevelClient = new RestHighLevelClient(builder);
    }

    static {
        bulkProcessor = createBulkProcessor();
    }

    private static BulkProcessor createBulkProcessor() {

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("xxx");
                log.info("1. 【beforeBulk】批次[{}] 携带 {} 请求数量", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (!response.hasFailures()) {
                    log.info("2. 【afterBulk-成功】批量 [{}] 完成在 {} ms", executionId, response.getTook().getMillis());
                } else {
                    BulkItemResponse[] items = response.getItems();
                    for (BulkItemResponse item : items) {
                        if (item.isFailed()) {
                            log.info("2. 【afterBulk-失败】批量 [{}] 出现异常的原因 : {}", executionId, item.getFailureMessage());
                            break;
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {

                List<DocWriteRequest<?>> requests = request.requests();
                List<String> esIds = requests.stream().map(DocWriteRequest::id).collect(Collectors.toList());
                log.error("3. 【afterBulk-failure失败】es执行bluk失败,失败的esId为：{}", esIds, failure);
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(((bulkRequest, bulkResponseActionListener) -> {
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
        }), listener);
        //到达10000条时刷新
        builder.setBulkActions(10000);
        //内存到达8M时刷新
        builder.setBulkSize(new ByteSizeValue(8L, ByteSizeUnit.MB));
        //设置的刷新间隔10s
        builder.setFlushInterval(TimeValue.timeValueSeconds(10));
        //设置允许执行的并发请求数。
        builder.setConcurrentRequests(8);
        //设置重试策略
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3));
        return builder.build();
    }

    /**
     * 异步插入es,未成功
     */
    @Test
    public void testBatchAsync() {
        List<IndexRequest> indexRequests = new ArrayList<>();
        ArrayList<User> users = new ArrayList<>();
        users.add(new User(13, "xiarun", 18, new Date(), "anhui"));
        users.add(new User(12, "zhutao", 18, new Date(), "anhui"));
        users.forEach(e -> {
            IndexRequest request = new IndexRequest("user");
            //填充id
            request.id(e.getId() + "");
            Map<String, Object> params = new HashMap<>();
            params.put("id", e.getId());
            params.put("name", e.getName());
            params.put("age", e.getAge());
            params.put("birthday", e.getBirthday());
            params.put("city", e.getCity());
            request.source(JSONObject.toJSONString(params), XContentType.JSON);
            request.opType(DocWriteRequest.OpType.CREATE);
            indexRequests.add(request);
        });
        indexRequests.forEach(bulkProcessor::add);
    }

    @Test
    public void testAsyncSingle() {
        IndexRequest indexRequest = new IndexRequest("test_demo");
        User user = new User(1001, "xiarun", 18, new Date(), "anhui");
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //数据为存储而不是更新
        indexRequest.create(false);
        indexRequest.id(user.getId() + "");
        restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        log.error("将id为：{}的数据存入ES时存在失败的分片，原因为：{}", indexRequest.id(), failure.getCause());
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("{}:存储es时异常，数据信息为", indexRequest.id(), e);
            }
        });
    }

    @Test
    public void testIndexIsExists() throws IOException {
        GetIndexRequest request = new GetIndexRequest("test_demo");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);// 索引是否存在
        restHighLevelClient.close();
    }

    @Test
    public void upsetMapIndex() throws IOException {
        // 获取链接
        BulkRequest request = new BulkRequest();
        // 设置索引名称，索引名称由系统编码和业务编码组成。若业务比较简单，也可以仅设置为一个字段。此处是为了方便多系统多业务使用
        request.routing("user");
        ArrayList<User> users = new ArrayList<>();
        users.add(new User(9, "xiarun", 18, new Date(), "anhui"));
        users.add(new User(10, "zhutao", 18, new Date(), "anhui"));
        for (User o : users) {
            // 实体类必须包含ES自定义主键，搜索引擎ES将根据自定义ID进行重复新验证。不需要按照字段顺序填写map
            Map<String, Object> params = new HashMap<>();
            params.put("age", o.getAge());
            params.put("city", o.getCity());
            params.put("birthday", o.getBirthday());
            params.put("id", o.getId());
            params.put("name", o.getName());
            String jsonStr = JSONObject.toJSONString(params);
            IndexRequest indexRequest = new IndexRequest("user").id(o.getId().toString()).source(jsonStr, XContentType.JSON);
            //OpType.CREATE：当存在相同的_id时，插入会出现异常；
            //OpType.INDEX：当存在相同_id时，插入会进行覆盖；
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            request.add(indexRequest);
        }
        // 批量新增数据
        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        RestStatus status = bulk.status();
        // 关闭链接
        restHighLevelClient.close();
        System.out.println(status);
    }


    @Test
    public void upsetMapIndexOne() throws IOException {
        // 获取链接
        BulkRequest request = new BulkRequest();
        // 设置索引名称，索引名称由系统编码和业务编码组成。若业务比较简单，也可以仅设置为一个字段。此处是为了方便多系统多业务使用
        //request.routing("test4");
        // 实体类必须包含ES自定义主键，搜索引擎ES将根据自定义ID进行重复新验证。不需要按照字段顺序填写map
        IndexRequest indexRequest = new IndexRequest("test4").id("9").source("{\"birthday\":\"1989-06-04T00:00" +
                ":00+0900\",\"sex\":false,\"birthday_ts\":\"1989-06-04T00:00:00+0900\",\"half_float_mum\":1.1," +
                "\"uuid\":12343232131313,\"float_num\":1.1,\"desc_varchar\":\"cccxx\",\"desc_char\":\"cccxx\",\"scaled_float_num\":1.1,\"identity\":1,\"name\":\"cccxx\",\"desc_string\":\"cccxx\",\"id\":32,\"age\":22,\"height\":1.1}", XContentType.JSON);
        //OpType.CREATE：当存在相同的_id时，插入会出现异常；
        //OpType.INDEX：当存在相同_id时，插入会进行覆盖；
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        DeleteRequest deleteRequest = new DeleteRequest("test4", "9");

        request.add(deleteRequest);
        request.add(indexRequest);
        // 批量新增数据
        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        RestStatus status = bulk.status();
        // 关闭链接
        restHighLevelClient.close();
        System.out.println(status);
    }

    @Test
    public void createIndexTest() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test5");
        CreateIndexResponse response = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        PutMappingRequest request = new PutMappingRequest("test5");
        request.source("{\"properties\":{\"birthday\":{\"type\":\"date\"},\"sex\":{\"type\":\"boolean\"},\"birthday_ts\":{\"type\":\"date\"},\"half_float_mum\":{\"type\":\"float\"},\"uuid\":{\"type\":\"long\"},\"float_num\":{\"type\":\"float\"},\"desc_varchar\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"desc_char\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"scaled_float_num\":{\"type\":\"float\"},\"identity\":{\"type\":\"long\"},\"desc_string\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},\"id\":{\"type\":\"long\"},\"age\":{\"type\":\"long\"},\"height\":{\"type\":\"float\"}}}", XContentType.JSON);
        AcknowledgedResponse putMappingResponse = restHighLevelClient.indices().putMapping(request, RequestOptions.DEFAULT);

        System.out.println(response.isAcknowledged());
        System.out.println(putMappingResponse.isAcknowledged());
    }

    @Test
    public void upsetEntityIndex() throws IOException {
        // 获取链接
        BulkRequest request = new BulkRequest();
        // 设置索引名称，索引名称由系统编码和业务编码组成。若业务比较简单，也可以仅设置为一个字段。此处是为了方便多系统多业务使用
        request.routing("user");
        ArrayList<User> demoDtos = new ArrayList<>();
        demoDtos.add(new User(1005, "xiarun", 18, new Date(), "anhui"));
        demoDtos.add(new User(1006, "zhutao", 18, new Date(), "anhui"));
        for (User o : demoDtos) {
            // 将数据转换为String类型，批量插入时设置为JSON格式
            String data = JSONObject.toJSONString(o);
            // 实体类必须包含ES自定义主键，搜索引擎ES将根据自定义ID进行重复新验证。
            // id为null时，在es中是随机id
            request.add(new IndexRequest("user").id(o.getId().toString()).source(data, XContentType.JSON));
        }
        // 批量新增数据
        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        RestStatus status = bulk.status();
        // 关闭链接
        restHighLevelClient.close();
        System.out.println(status);
    }

    @Test
    public void getMapping() throws IOException {
        // 获取链接
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices("test3");
        // 批量新增数据
        GetMappingsResponse response = restHighLevelClient.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
        // 关闭链接
        restHighLevelClient.close();
        Map<String, MappingMetaData> allMappings = response.mappings();
        MappingMetaData indexMapping = allMappings.get("test3");
        Map<String, Object> mapping = indexMapping.sourceAsMap();
        mapping.size();
        for (Map.Entry<String, Object> properties : mapping.entrySet()) {
            System.out.println(properties.getKey());
            System.out.println(properties.getValue());
            JSONObject property = JSONObject.parseObject(JSON.toJSONString(properties.getValue()));
            JSONObject birthday = (JSONObject) property.get("birthday");
            Object type = ((JSONObject) property.get("birthday")).get("type");
            System.out.println(type);
        }

    }

    @Test
    public void testClusterInfo() throws IOException {
        MainResponse response = null;
        try {
            response = restHighLevelClient.info(RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //返回集群的各种信息
        String clusterName = response.getClusterName(); //集群名称
        String clusterUuid = response.getClusterUuid(); //群集的唯一标识符
        String nodeName = response.getNodeName(); //已执行请求的节点的名称
        MainResponse.Version version = response.getVersion(); //已执行请求的节点的版本
    }

    @Test
    public void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("test4", "37");
        request.timeout("1s");
        DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());// OK
    }

}

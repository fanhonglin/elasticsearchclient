package org.es.demo;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class EsService {
    @Autowired
    RestHighLevelClient client;


//    public void processNormalQueryObj(List<EsProductDetail> itemDetailListEs, RecRequest recRequest, EsQueryObject esQueryObject){
////        preProcessQueryObj(itemDetailListEs,  recRequest, esQueryObject);
//        int pageSize=recRequest.getPageSize();
//        esQueryObject.setSize(pageSize);
//        Integer from = recRequest.getPageNo();
//        if (from == null || from <= 0) {
//            from = 1;
//        }
//        esQueryObject.setFrom((from - 1) * pageSize);
//        processQueryObject(recRequest,esQueryObject);
//    }


//    //单条写入索引
//    public void singleIndexDoc(Map<String, Object> dataMap, String indexName, String indexId) {
//        IndexRequest indexRequest = new IndexRequest(indexName).id(indexId).source(dataMap);//构建IndexRequest对象并设置对应的索引和_id字段名称
//        try {
//            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);//执行写入
//            String index = indexResponse.getIndex();//通过IndexResponse获取索引名称
//            String id = indexResponse.getId();//通过IndexResponse获取文档Id
//            Long version = indexResponse.getVersion();//通过IndexResponse获取文档版本
//            System.out.println("index=" + index + ",id=" + id + ",version=" + version );
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    //批量写入索引
//    public void bulkIndexDoc(String indexName, String docIdKey, List<Map<String, Object>> recordMapList) {
//        BulkRequest bulkRequest = new BulkRequest(indexName);//构建批量操作BulkRequest对象
//        for (Map<String, Object> dataMap : recordMapList) {//遍历数据
//            String docId = dataMap.get(docIdKey).toString();//获取主键作为Elasticsearch索引的主键
//            IndexRequest indexRequest = new IndexRequest().id(docId).source(dataMap);//构建IndexRequest对象
//            bulkRequest.add(indexRequest);//添加IndexRequest
//        }
//        bulkRequest.timeout(TimeValue.timeValueSeconds(5));//设置超时时间
//        try {
//            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);//执行批量写入
//            if (bulkResponse.hasFailures()) {//判断执行状态
//                System.out.println("bulk fail,message:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //批量写入文档增加监听器
//    public void bulkListenerIndexDoc(String indexName, String docIdKey, List<Map<String, Object>> recordMapList) {
//        //新建批量请求监听器
//        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
//            @Override
//            public void beforeBulk(long executionId, BulkRequest request) {
//                int numberOfActions = request.numberOfActions();
//                System.out.println("批量请求ID:" + executionId + "，本批次大小:" + numberOfActions);
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//                if (response.hasFailures()) {
//                    System.out.println("批量请求失败,ID:" + executionId);
//                } else {
//                    System.out.println("批量请求完成,ID:" + executionId + "，耗时(毫秒):" + response.getTook().getMillis());
//                }
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//                System.out.println("批量请求失败,ID:" + executionId + ",异常原因:" + failure);
//            }
//        };
//        //新建批量请求builder
//        BulkProcessor.Builder builder = BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
//        builder.setGlobalIndex(indexName);//全局设置索引名称
//        builder.setBulkActions(500);//设置批次数量
//        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));//设置批次大小
//        builder.setConcurrentRequests(0);//设置关闭并发
//        builder.setFlushInterval(TimeValue.timeValueSeconds(10));//设置刷新时间
//        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3));//设置补偿策略
//        BulkProcessor bulkProcessor = builder.build();
//        for (Map<String, Object> dataMap : recordMapList) {//遍历数据
//            String docId = dataMap.get(docIdKey).toString();//获取主键作为Elasticsearch索引的主键
//            IndexRequest indexRequest = new IndexRequest().id(docId).source(dataMap);//构建索引插入请求
//            bulkProcessor.add(indexRequest);//添加请求
//        }
//        try {
//            bulkProcessor.awaitClose(3, TimeUnit.SECONDS);//执行批量写入操作
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //-----------------------------------------------------------------------------------------------------------------------------------
//    //单条update
//    public void singleUpdate(String indexName, String docIdKey, Map<String, Object> recordMap) {
//        UpdateRequest updateRequest = new UpdateRequest(indexName, docIdKey);
//        updateRequest.doc(recordMap);
//        try {
//            UpdateResponse updateResponse=client.update(updateRequest, RequestOptions.DEFAULT);
//            String index = updateResponse.getIndex();//通过IndexResponse获取索引名称
//            String id = updateResponse.getId();//通过IndexResponse获取文档Id
//            Long version = updateResponse.getVersion();//通过IndexResponse获取文档版本
//            System.out.println("index=" + index + ",id=" + id + ",version="+version);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    //批量update文档
//    public void bulkUpdate(String index, String docIdKey, List<Map<String, Object>> recordMapList) {
//        BulkRequest bulkRequest = new BulkRequest();//构建BulkRequest对象
//        for (Map<String, Object> dataMap : recordMapList) {//遍历数据列表
//            String docId = dataMap.get(docIdKey).toString();
//            dataMap.remove(docId);//将id字段从map中删除
//            bulkRequest.add(new UpdateRequest(index, docId).doc(dataMap));//创建UpdateRequest对象
//        }
//        try {
//            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);//执行批量更新
//            if (bulkResponse.hasFailures()) {//判断状态
//                System.out.println("bulk fail,message:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    //update by query
//    public void updateCityByQuery(String index,String oldCity,String newCity) {
//        UpdateByQueryRequest updateByQueryRequest=new UpdateByQueryRequest(index);//构建UpdateByQueryRequest对象
//        updateByQueryRequest.setQuery(new TermQueryBuilder("city",oldCity));//设置按照城市查找文档的query
//        updateByQueryRequest.setScript(new Script("ctx._source['city']='"+newCity+"';"));//设置更新城市字段的脚本逻辑
//        try {
//            client.updateByQuery(updateByQueryRequest,RequestOptions.DEFAULT);//执行更新
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    //单条upsert文档
//    public void singleUpsert(String index, String docIdKey, Map<String, Object> recordMap,Map<String, Object> upRecordMap) {
//        UpdateRequest updateRequest = new UpdateRequest(index, docIdKey);//构建UpdateRequest
//        updateRequest.doc(recordMap);//设置写更新辑
//        updateRequest.upsert(upRecordMap);//设置插入逻辑
//        try {
//            client.update(updateRequest, RequestOptions.DEFAULT);//执行upsert
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //批量upsert文档
//    public void bulkUpsert(String indexName, String docIdKey, List<Map<String, Object>> recordMapList) {
//        BulkRequest bulkRequest = new BulkRequest();//新建请求
//        //遍历所有的提示词和提示词对应的拼音形式数据
//        for (Map<String, Object> dataMap : recordMapList) {
//            String docId = dataMap.get(docIdKey).toString();//获取主键作为Elasticsearch索引的主键
//            UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(dataMap, XContentType.JSON).upsert(dataMap, XContentType.JSON);//构建写入请求
//            bulkRequest.add(updateRequest);//批量加入写入请求
//        }
//        try {
//            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);//获取批量写入的返回结果
//            if (bulkResponse.hasFailures()) {//判断是否写入失败
//                System.out.println("bulk fail,message:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    //-----------------------------------------------------------------------------------------------------------------------------------------------------------
//    //单条删除索引
//    //单条update
//    public void singleDelete(String index, String docId) {
//        DeleteRequest deleteRequest=new DeleteRequest(index,docId);//构建删除请求
//        try {
//            client.delete(deleteRequest, RequestOptions.DEFAULT);//执行删除
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    //批量delete文档
//    public void bulkDelete(String index, String docIdKey, List<String> docIdList) {
//        BulkRequest bulkRequest = new BulkRequest();//构建BulkRequest对象
//        for (String docId : docIdList) {//遍历文档_d列表
//            DeleteRequest deleteRequest=new DeleteRequest(index,docId);//构建删除请求
//            bulkRequest.add(deleteRequest);//创建UpdateRequest对象
//        }
//        try {
//            BulkResponse bulkResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);//执行批量删除
//            if (bulkResponse.hasFailures()) {//判断状态
//                System.out.println("bulk fail,message:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    //update by query
//    public void deleteByQuery(String index,String city) {
//        DeleteByQueryRequest deleteByQueryRequest=new DeleteByQueryRequest(index);//构建DeleteByQueryRequest对象
//        deleteByQueryRequest.setQuery(new TermQueryBuilder("city",city));//设置按照城市查找文档的query
//        try {
//            client.deleteByQuery(deleteByQueryRequest,RequestOptions.DEFAULT);//执行删除
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public List<Hotel> getHotelFromTitle(String keyword) {
//        SearchRequest searchRequest = new SearchRequest("hotel");//客户端请求
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchQuery("title", keyword));//构建query
//        searchRequest.source(searchSourceBuilder);
//        List<Hotel> resultList = new ArrayList<Hotel>();
//        try {
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            RestStatus status = searchResponse.status();
//            if (status != RestStatus.OK) {
//                return null;
//            }
//            SearchHits searchHits = searchResponse.getHits();
//            for (SearchHit searchHit : searchHits) {
//                Hotel hotel = new Hotel();
//                hotel.setId(searchHit.getId());//文档_id
//                hotel.setIndex(searchHit.getIndex());//索引名称
//                hotel.setScore(searchHit.getScore());//文档得分
//
//                Map<String, Object> dataMap = searchHit.getSourceAsMap();//转换为Map
//                hotel.setTitle((String) dataMap.get("title"));//设置标题
//                hotel.setCity((String) dataMap.get("city"));//设置城市
//                hotel.setPrice((Double) dataMap.get("price"));//设置价格
//                resultList.add(hotel);
//            }
//            return resultList;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
}

package org.es.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.range.*;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EsAggService {
    @Autowired
    RestHighLevelClient client;

    public void geGeoDistanceAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String geoDistanceAggName = "location";//sum聚合的名称
        //定义GeoDistance聚合
        GeoDistanceAggregationBuilder geoDistanceAgg=AggregationBuilders.geoDistance(geoDistanceAggName,new GeoPoint(39.915143,116.4039));
        geoDistanceAgg.unit(DistanceUnit.KILOMETERS);
        geoDistanceAgg.field("location");
        //指定分桶范围规则
        geoDistanceAgg.addRange(new GeoDistanceAggregationBuilder.Range(null,0d,3d));
        geoDistanceAgg.addRange(new GeoDistanceAggregationBuilder.Range(null,3d,110d));
        geoDistanceAgg.addRange(new GeoDistanceAggregationBuilder.Range(null,110d,null));
        String minAggName = "my_min";//min聚合的名称
        //定义sum聚合，指定字段为价格
        MinAggregationBuilder minAgg = AggregationBuilders.min(minAggName).field("price");
        minAgg.missing(100);//指定默认值
        //定义聚合的父子关系
        geoDistanceAgg.subAggregation(minAgg);
        searchSourceBuilder.aggregation(geoDistanceAgg);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        ParsedGeoDistance range = aggregations.get(geoDistanceAggName);//获取GeoDistance聚合返回的对象
        for (Range.Bucket  bucket : range.getBuckets()) {
            String termsKey = bucket.getKeyAsString();//获取bucket名称的字符串形式
            System.out.println("termsKey=" + termsKey);
            ParsedMin min = bucket.getAggregations().get(minAggName);
            String key = min.getName();//获取聚合名称
            double minVal = min.getValue();//获取聚合值
            System.out.println("key=" + key + ",min=" + minVal);//打印结果
        }
    }

    public void getCollapseAggSearch() throws IOException{
        //按照spu进行分组
        CollapseBuilder collapseBuilder = new CollapseBuilder("city");//按照城市进行分组
        SearchRequest searchRequest = new SearchRequest();//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "金都"));//新建match查询
        searchSourceBuilder.collapse(collapseBuilder);//设置折叠
        searchRequest.source(searchSourceBuilder);//设置查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
        for (SearchHit searchHit : searchHits) {//遍历搜索结果集
            String index = searchHit.getIndex();//获取索引名称
            String id = searchHit.getId();//获取文档_id
            Float score = searchHit.getScore();//获取得分
            String source = searchHit.getSourceAsString();//获取文档内容
            System.out.println("index=" + index + ",id=" + id + ",score=" + score + ",source=" + source);//打印数据
        }
    }


    public void printResult(SearchRequest searchRequest) {
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
            SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
            for (SearchHit searchHit : searchHits) {//遍历搜索结果集
                String index = searchHit.getIndex();//获取索引名称
                String id = searchHit.getId();//获取文档_id
                Float score = searchHit.getScore();//获取得分
                String source = searchHit.getSourceAsString();//获取文档内容
                System.out.println("index=" + index + ",id=" + id + ",score=" + score + ",source=" + source);//打印数据
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getAggTopHitsSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("city");
        BucketOrder bucketOrder=BucketOrder.key(true);
        termsAggregationBuilder.order(bucketOrder);

        String topHitsAggName = "my_top";//avg聚合的名称
        TopHitsAggregationBuilder topHitsAgg=AggregationBuilders.topHits(topHitsAggName);
        topHitsAgg.size(3);
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(topHitsAgg);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchSourceBuilder.query(QueryBuilders.matchQuery("title","金都"));
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索

        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取sum聚合返回的对象
        for (Terms.Bucket bucket : terms.getBuckets()) {
            String bucketKey = bucket.getKey().toString();
            System.out.println("termsKey=" + bucketKey);
            TopHits topHits = bucket.getAggregations().get(topHitsAggName);
            SearchHit[] searchHits=topHits.getHits().getHits();
            for(SearchHit searchHit:searchHits){
                System.out.println(searchHit.getSourceAsString());
            }
        }
    }

    public void getAggKeyOrderSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称
        //定义terms聚合，指定字段为城市
        String avgAggName = "my_avg";//avg聚合的名称
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("city");
        BucketOrder bucketOrder=BucketOrder.key(true);
        termsAggregationBuilder.order(bucketOrder);

        //定义sum聚合，指定字段为价格
        SumAggregationBuilder avgAgg = AggregationBuilders.sum(avgAggName).field("price");
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(avgAgg);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取sum聚合返回的对象
        for (Terms.Bucket bucket : terms.getBuckets()) {
            String bucketKey = bucket.getKey().toString();
            System.out.println("termsKey=" + bucketKey);
            Sum sum = bucket.getAggregations().get(avgAggName);
            String key = sum.getName();//获取聚合名称
            double sumVal = sum.getValue();//获取聚合值
            System.out.println("key=" + key + ",count=" + sumVal);//打印结果
        }
    }


    public void getAggMetricsOrderSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称
        //定义terms聚合，指定字段为城市
        String avgAggName = "my_avg";//avg聚合的名称
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("city");
        BucketOrder bucketOrder=BucketOrder.aggregation(avgAggName,true);
        termsAggregationBuilder.order(bucketOrder);

        //定义sum聚合，指定字段为价格
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg(avgAggName).field("price");
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(avgAgg);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取聚合返回的对象
        for (Terms.Bucket bucket : terms.getBuckets()) {
            String bucketKey = bucket.getKey().toString();
            System.out.println("termsKey=" + bucketKey);
            Avg avg = bucket.getAggregations().get(avgAggName);
            String key = avg.getName();//获取聚合名称
            double avgVal = avg.getValue();//获取聚合值
            System.out.println("key=" + key + ",avgVal=" + avgVal);//打印结果
        }
    }

    public void getAggDocCountOrderSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称
        //定义terms聚合，指定字段为城市
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("city");
        BucketOrder bucketOrder=BucketOrder.count(true);
        termsAggregationBuilder.order(bucketOrder);
        String avgAggName = "my_avg";//avg聚合的名称
        //定义sum聚合，指定字段为价格
        SumAggregationBuilder avgAgg = AggregationBuilders.sum(avgAggName).field("price");
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(avgAgg);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取sum聚合返回的对象
        for (Terms.Bucket bucket : terms.getBuckets()) {
            String bucketKey = bucket.getKey().toString();
            System.out.println("termsKey=" + bucketKey);
            Sum sum = bucket.getAggregations().get(avgAggName);
            String key = sum.getName();//获取聚合名称
            double sumVal = sum.getValue();//获取聚合值
            System.out.println("key=" + key + ",count=" + sumVal);//打印结果
        }
    }


    public void getPostFilterAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String avgAggName = "my_avg";//avg聚合的名称
        //定义sum聚合，指定字段为价格
        AvgAggregationBuilder avgAgg= AggregationBuilders.avg(avgAggName).field("price");
        avgAgg.missing(200);//设置默认值为200
        searchSourceBuilder.aggregation(avgAgg);//添加聚合
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "假日"));//构建term查询
        TermQueryBuilder termQueryBuilder=QueryBuilders.termQuery("city","北京");
        searchSourceBuilder.postFilter(termQueryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Avg avg =aggregations.get(avgAggName);
        String key = avg.getName();//获取聚合名称
        double avgVal = avg.getValue();//获取聚合值
        System.out.println("key=" + key + ",avgVal=" + avgVal);//打印结果
    }


    public void getFilterAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String filterAggName = "my_terms";//sum聚合的名称
        TermQueryBuilder termQueryBuilder=QueryBuilders.termQuery("full_room",true);
        FilterAggregationBuilder filterAggregationBuilder=AggregationBuilders.filter(filterAggName,termQueryBuilder);

        String avgAggName = "my_avg";//avg聚合的名称
        //定义sum聚合，指定字段为价格
        AvgAggregationBuilder avgAgg= AggregationBuilders.avg(avgAggName).field("price");

        filterAggregationBuilder.subAggregation(avgAgg);//为filter聚合添加子聚合
        searchSourceBuilder.aggregation(filterAggregationBuilder);//添加聚合
        searchSourceBuilder.query(QueryBuilders.termQuery("city", "北京"));//构建term查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        ParsedFilter filter = aggregations.get(filterAggName);//获取sum聚合返回的对象
        Avg avg =filter.getAggregations().get(avgAggName);
        String key = avg.getName();//获取聚合名称
        double avgVal = avg.getValue();//获取聚合值
        System.out.println("key=" + key + ",avgVal=" + avgVal);//打印结果
    }


    public void getQueryAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String avgAggName = "my_avg";//avg聚合的名称
        //定义sum聚合，指定字段为价格
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg(avgAggName).field("price");

        searchSourceBuilder.aggregation(avgAgg);//添加聚合
        searchSourceBuilder.query(QueryBuilders.termQuery("city", "北京"));//构建query查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
        SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
        System.out.println("---------------hit--------------");
        for (SearchHit searchHit : searchHits) {//遍历搜索结果集
            String index = searchHit.getIndex();//获取索引名称
            String id = searchHit.getId();//获取文档_id
            Float score = searchHit.getScore();//获取得分
            String source = searchHit.getSourceAsString();//获取文档内容
            System.out.println("index=" + index + ",id=" + id + ",source=" + source);//打印数据
        }
        System.out.println("---------------agg--------------");
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        ParsedAvg avg = aggregations.get(avgAggName);//获取sum聚合返回的对象
        String avgName = avg.getName();//获取聚合名称
        double avgVal = avg.getValue();//获取聚合值
        System.out.println("avgName=" + avgName + ",avgVal=" + avgVal);//打印结果
    }

    public void getRangeDocCountAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String rangeAggName = "my_range";//sum聚合的名称
        //定义range聚合，指定字段为price
        RangeAggregationBuilder rangeAgg = AggregationBuilders.range(rangeAggName).field("price");
        rangeAgg.addRange(new RangeAggregator.Range(null,null,200d));
        rangeAgg.addRange(new RangeAggregator.Range(null,200d,500d));
        rangeAgg.addRange(new RangeAggregator.Range(null,500d,null));
        searchSourceBuilder.aggregation(rangeAgg);//添加range聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Range range = aggregations.get(rangeAggName);//获取range聚合返回的对象
        for (Range.Bucket bucket : range.getBuckets()) {
            String bucketKey = bucket.getKeyAsString();//获取桶名称
            long docCount = bucket.getDocCount();//获取聚合文档个数
            System.out.println("bucketKey=" + bucketKey + ",docCount=" + docCount);
        }
    }

    public void getBucketDocCountAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//指定聚合的名称
        //定义terms聚合，指定字段为城市
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("full_room");

        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取聚合返回的对象

        for (Terms.Bucket bucket : terms.getBuckets()) {
            String bucketKey = bucket.getKeyAsString();//获取桶名称
            long docCount = bucket.getDocCount();//获取文档个数
            System.out.println("termsKey=" + bucketKey + ",docCount=" + docCount);
        }
    }


    public void getBucketStrAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称
        //定义terms聚合，指定字段为满房状态
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("full_room");
        String sumAggName = "my_sum";//sum聚合的名称
        //定义sum聚合，指定字段为价格
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum(sumAggName).field("price");
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取sum聚合返回的对象

        for (Terms.Bucket bucket : terms.getBuckets()) {
            String termsKey = bucket.getKeyAsString();//获取bucket名称的字符串形式
            System.out.println("termsKey=" + termsKey);
            Sum sum = bucket.getAggregations().get(sumAggName);
            String key = sum.getName();//获取聚合名称
            double sumVal = sum.getValue();//获取聚合值
            System.out.println("key=" + key + ",count=" + sumVal);//打印结果
        }
    }


    public void getExternalBucketAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String aggNameCity = "my_terms_city";//按城市聚合的名称
        //定义terms聚合，指定字段为城市
        TermsAggregationBuilder termsAggCity = AggregationBuilders.terms(aggNameCity).field("city");

        String aggNameFullRoom = "my_terms_full_room";//按满房状态聚合的名称
        //定义terms聚合，指定字段为满房状态
        TermsAggregationBuilder termsArrFullRoom = AggregationBuilders.terms(aggNameCity).field("full_room");

        String sumAggName = "my_sum";//sum聚合的名称
        //定义sum聚合，指定字段为价格
        SumAggregationBuilder sumAgg = AggregationBuilders.sum(sumAggName).field("price");

        //定义聚合的父子关系
        termsArrFullRoom.subAggregation(sumAgg);
        termsAggCity.subAggregation(termsArrFullRoom);
        searchSourceBuilder.aggregation(termsAggCity);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(aggNameCity);//获取sum聚合返回的对象
        for (Terms.Bucket bucket : terms.getBuckets()) {//遍历第一层bucket
            String termsKeyCity = bucket.getKey().toString();//获取第一层bucket名称
            System.out.println("--------" + "termsKeyCity=" + termsKeyCity + "--------");
            Terms termsFullRom = bucket.getAggregations().get(aggNameCity);
            for (Terms.Bucket bucketFullRoom : termsFullRom.getBuckets()) {//遍历第二层bucket
                String termsKeyFullRoom = bucketFullRoom.getKeyAsString();//获取第二层bucket名称
                System.out.println("termsKeyFullRoom=" + termsKeyFullRoom);
                Sum sum = bucketFullRoom.getAggregations().get(sumAggName);//获取聚合指标
                String key = sum.getName();//获取聚合指标名称
                double sumVal = sum.getValue();//获取聚合指标值
                System.out.println("key=" + key + ",count=" + sumVal);//打印结果
            }
        }
    }


    public void getBucketAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termsAggName = "my_terms";//sum聚合的名称
        //定义terms聚合，指定字段为城市
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(termsAggName).field("city");
        String sumAggName = "my_sum";//sum聚合的名称
        //定义sum聚合，指定字段为价格
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum(sumAggName).field("price");
        //定义聚合的父子关系
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Terms terms = aggregations.get(termsAggName);//获取sum聚合返回的对象

        for (Terms.Bucket bucket : terms.getBuckets()) {
            String termsKey = bucket.getKey().toString();
            System.out.println("termsKey=" + termsKey);
            Sum sum = bucket.getAggregations().get(sumAggName);
            String key = sum.getName();//获取聚合名称
            double sumVal = sum.getValue();//获取聚合值
            System.out.println("key=" + key + ",count=" + sumVal);//打印结果
        }
    }


    public void getAvgAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String aggName = "my_agg";//聚合的名称
        //定义avg聚合，指定字段为价格
        AvgAggregationBuilder aggregationBuilder = AggregationBuilders.avg(aggName).field("price");
        searchSourceBuilder.aggregation(aggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Avg avg = aggregations.get(aggName);//获取avg聚合返回的对象
        String key = avg.getName();//获取聚合名称
        double avgValue = avg.getValue();//获取聚合值
        System.out.println("key=" + key + ",avgValue=" + avgValue);//打印结果
    }

    public void getSumAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String aggName = "my_agg";//聚合的名称
        //定义sum聚合，指定字段为价格
        SumAggregationBuilder aggregationBuilder = AggregationBuilders.sum(aggName).field("price");
        aggregationBuilder.missing("200");
        searchSourceBuilder.aggregation(aggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Sum sum = aggregations.get(aggName);//获取sum聚合返回的对象
        String key = sum.getName();//获取聚合名称
        double sumVal = sum.getValue();//获取聚合值
        System.out.println("key=" + key + ",count=" + sumVal);//打印结果
    }

    public void getStatsAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String aggName = "my_agg";//聚合的名称
        //定义stats聚合，指定字段为价格
        StatsAggregationBuilder aggregationBuilder = AggregationBuilders.stats(aggName).field("price");
        searchSourceBuilder.aggregation(aggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        Stats stats = aggregations.get(aggName);//获取stats聚合返回的对象
        String key = stats.getName();//获取聚合名称
        double sumVal = stats.getSum();//获取聚合加和值
        double avgVal = stats.getAvg();//获取聚合平均值
        long countVal = stats.getCount();//获取聚合文档数量值
        double maxVal = stats.getMax();//获取聚合最大值
        double minVal = stats.getMin();//获取聚合最小值
        System.out.println("key=" + key);//打印聚合名称
        System.out.println("sumVal=" + sumVal + ",avgVal=" + avgVal + ",countVal=" + countVal + ",maxVal=" + maxVal + ",minVal=" + minVal);//打印结果
    }

    public void getValueCountAggSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String aggName = "my_agg";//聚合的名称
        //定义value_count聚合，指定字段为价格
        ValueCountAggregationBuilder aggregationBuilder = AggregationBuilders.count(aggName).field("price");
        aggregationBuilder.missing("200");
        searchSourceBuilder.aggregation(aggregationBuilder);//添加聚合
        searchRequest.source(searchSourceBuilder);//设置查询请求
        //执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();//获取聚合结果
        ValueCount valueCount = aggregations.get(aggName);//获取value_count聚合返回的对象
        String key = valueCount.getName();//获取聚合名称
        long count = valueCount.getValue();//获取聚合值
        System.out.println("key=" + key + ",count=" + count);//打印结果
    }

}

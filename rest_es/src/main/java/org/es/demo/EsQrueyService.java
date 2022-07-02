package org.es.demo;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.*;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class EsQrueyService {
    @Autowired
    RestHighLevelClient client;

    public void getBoostSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建"金都"的match查询
        MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("title", "金都");
        //设置boost值为2
        matchQueryBuilder1.boost(2);
        //构建"文雅"的match查询,boost使用默认值
        MatchQueryBuilder matchQueryBuilder2 = QueryBuilders.matchQuery("title", "文雅");
        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        //将"金都"的match查询添加到布尔查询
        boolQueryBuilder.should(matchQueryBuilder1);
        //将"文雅"的match查询添加到布尔查询
        boolQueryBuilder.should(matchQueryBuilder2);
        searchSourceBuilder.query(boolQueryBuilder);//设置查询为布尔查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }
    public void getBoostingSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建"金都"的match查询
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "金都");
        //构建价格的range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").lte("200");
        //构建满房的term查询
        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("full_room",true);
        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        //将价格的range查询添加到布尔查询
        boolQueryBuilder.should(rangeQueryBuilder);
        //将满房的term查询添加到布尔查询
        boolQueryBuilder.should(termQueryBuilder);
        //构建boosting查询,match查询作为正向查询,布尔查询作为负向查询
        BoostingQueryBuilder boosting=QueryBuilders.boostingQuery(matchQueryBuilder,boolQueryBuilder);
        boosting.negativeBoost(0.2f);//设置负向查询的系数
        searchSourceBuilder.query(boosting);//设置查询为boosting查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }
    public void getScriptScoreQuery() {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "金都");
        //编写脚本代码
        String scoreScript =
                "int weight=10;\n" +
                 "def random= randomScore(params.uuidHash);\n" +
                 " return weight*random;";
        Map paraMap = new HashMap();
        paraMap.put("uuidHash", 234537);//设置传递到脚本的参数
        //创建脚本对象

        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", scoreScript, paraMap);
        //创建ScriptScore查询builder
        ScriptScoreQueryBuilder scriptScoreQueryBuilder = QueryBuilders.scriptScoreQuery(matchQueryBuilder, script);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(scriptScoreQueryBuilder);
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }


    public void getRescoreQuery(){
        //构建原始的range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").gte("300");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);//添加原始查询Builder
        //构建二次打分的查询
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "金都");
        //构建二次打分Builder
        QueryRescorerBuilder queryRescorerBuilder = new QueryRescorerBuilder(matchQueryBuilder);
        queryRescorerBuilder.setQueryWeight(0.6f);//设置原始打分权重
        queryRescorerBuilder.setRescoreQueryWeight(1.7f);//设置二次打分权重
        queryRescorerBuilder.windowSize(3);//设置每个分片的参加二次打分文档个数
        searchSourceBuilder.addRescorer(queryRescorerBuilder);//添加二次打分Builder
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }

//    public List<Hotel> testTermQuery() {
//        SearchRequest searchRequest = new SearchRequest("hotel");//客户端请求
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//创建搜索builder
//        TermQueryBuilder termQueryBuilder= QueryBuilders.term
//        searchSourceBuilder.query(new TermQueryBuilder());//构建query
//        searchRequest.source(searchSourceBuilder);
//        List<Hotel> resultList = new ArrayList<Hotel>();
//        try {
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
//            return resultList;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    public void termDateSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("create_time", "20210509160000"));//构建term查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }

    public void termsSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termsQuery("city", "北京", "天津"));//构建terms查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }

    public void rangeSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建range查询
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("create_time").gte("20210115120000").lte("20210116120000");
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }

    public void existsSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.existsQuery("tag"));
        searchRequest.source(searchSourceBuilder);
        printResult(searchRequest);
    }

    public void mustSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        TermQueryBuilder termQueryIsReady = QueryBuilders.termQuery("city", "北京");//构建城市term查询
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("price").gte(350).lte(400);//构建价格range查询
        boolQueryBuilder.must(termQueryIsReady).must(rangeQueryBuilder);//进行关系“与”查询
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void shouldSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        TermQueryBuilder termQueryIsReady = QueryBuilders.termQuery("city", "北京");//构建城市为“北京”的term查询
        TermQueryBuilder termQueryWritter = QueryBuilders.termQuery("city", "天津");//构建城市为“天津”的term查询
        boolQueryBuilder.should(termQueryIsReady).should(termQueryWritter);//进行关系“或”查询
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void mustNotSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        TermQueryBuilder termQueryIsReady = QueryBuilders.termQuery("city", "北京");//构建城市为“北京”的term查询
        TermQueryBuilder termQueryWritter = QueryBuilders.termQuery("city", "天津");//构建城市为“天津”的term查询
        boolQueryBuilder.mustNot(termQueryIsReady).mustNot(termQueryWritter);//进行关系“必须不”查询
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void filterSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.filter(QueryBuilders.termQuery("city", "北京"));
        boolQueryBuilder.filter(QueryBuilders.termQuery("full_room", false));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        printResult(searchRequest);
    }

    public void constantScoreSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(QueryBuilders.termQuery("amenities", "停车场"));//构建城市为“北京”的term查询
        searchSourceBuilder.query(constantScoreQueryBuilder);
        constantScoreQueryBuilder.boost(2.0f);
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void functionScoreSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQuery = QueryBuilders.termQuery("city", "北京");//构建term查询
        ScoreFunctionBuilder<?> scoreFunction = ScoreFunctionBuilders.randomFunction();//构建随机函数
        //构建function_score查询
        FunctionScoreQueryBuilder funcQuery = QueryBuilders.functionScoreQuery(termQuery, scoreFunction).boostMode(CombineFunction.SUM);
        searchSourceBuilder.query(funcQuery);
        searchRequest.source(searchSourceBuilder);//设置查询请求
        printResult(searchRequest);//打印搜索结果
    }

    public void matchAllSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery().boost(2.0f);//新建match_all查询，并设置boost值为2.0
        searchSourceBuilder.query(matchAllQueryBuilder);
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void matchSearch() {
        SearchRequest searchRequest = new SearchRequest();//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "金都").operator(Operator.AND));//新建match查询，并设置operator值为and
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void multiMatchSearch() {
        SearchRequest searchRequest = new SearchRequest();//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("假日", "title", "amenities"));//新建multi_match查询，从"title"和"amenities"字段查询"假日"
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void geoDistanceSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //新建geo_distance查询，设置基准点坐标和周边距离
        searchSourceBuilder.query(QueryBuilders.geoDistanceQuery("location").distance(5, DistanceUnit.KILOMETERS).point(40.026919, 116.47473));
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

    public void suggestSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel_sug");//创建搜索请求,指定索引名称为hotel_sug
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //创建completion类型搜索建议
        CompletionSuggestionBuilder comSuggest = SuggestBuilders.completionSuggestion("query_word").prefix("如家");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("hotel_zh_sug", comSuggest);//添加搜索建议，"hotel_zh_sug"为自定义名称
        searchSourceBuilder.suggest(suggestBuilder);//设置suggest请求
        searchRequest.source(searchSourceBuilder);//设置查询请求
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);//进行搜索,获取搜索结果
        CompletionSuggestion suggestion = response.getSuggest().getSuggestion("hotel_zh_sug");//获取suggest结果
        System.out.println("sug result:");
        //遍历suggest结果，并进行打印
        for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
            System.out.println("sug:" + option.getText().string());
        }
    }


    public void hightLightSearch() {
        SearchRequest searchRequest = new SearchRequest();//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "金都").operator(Operator.AND));//新建match查询，并设置operator值为and
        searchRequest.source(searchSourceBuilder);//设置查询
        HighlightBuilder highlightBuilder = new HighlightBuilder();//新建高亮搜索
        highlightBuilder.preTags("<high>");//设置高亮标签前缀
        highlightBuilder.postTags("</high>");//设置高亮标签后缀
        highlightBuilder.field("title");//设置高亮字段
        searchSourceBuilder.highlighter(highlightBuilder);//设置高亮搜索
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
            SearchHits searchHits = searchResponse.getHits();//获取搜索结果集
            for (SearchHit searchHit : searchHits) {//遍历搜索结果集
                Text[] texts = searchHit.getHighlightFields().get("title").getFragments();//得到高亮搜索结果
                for (Text text : texts) {//遍历高亮搜索
                    System.out.println(text);//打印每一个高亮结果
                }
                System.out.println("---------------------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void geoDistanceSearchSort() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //创建geo_distance查询，搜索距离中心点5公里范围内的酒店
        searchSourceBuilder.query(QueryBuilders.geoDistanceQuery("location").distance(5, DistanceUnit.KILOMETERS).point(39.915143, 116.4039));
        //创建geo_distance_sort排序，设定按照与中心点的距离升序排序
        GeoDistanceSortBuilder geoDistanceSortBuilder = SortBuilders.geoDistanceSort("location", 39.915143, 116.4039)
                .point(39.915143, 116.4039).unit(DistanceUnit.KILOMETERS).order(SortOrder.ASC);
        searchSourceBuilder.sort(geoDistanceSortBuilder);//设置排序规则
        searchRequest.source(searchSourceBuilder);//设置查询
        //开始搜索
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits searchHits = searchResponse.getHits();//获取搜索结果
        System.out.println("search result distance sort:");
        //开始遍历搜索结果
        for (SearchHit searchHit : searchHits) {
            //得到酒店距离中心点的距离
            double geoDistance = (double) searchHit.getSortValues()[0];
            //以Map形式获取文档_source内容
            Map<String, Object> sourceMap = searchHit.getSourceAsMap();
            Object title = sourceMap.get("title");
            Object city = sourceMap.get("city");
            //打印结果
            System.out.println("title=" + title + ",city=" + city + ",geoDistance:" + geoDistance);
        }
    }

    public void commonSort() {
        SearchRequest searchRequest = new SearchRequest("hotel");//创建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "金都"));//构建match查询
        searchRequest.source(searchSourceBuilder);//设置查询请求
        searchSourceBuilder.sort("price", SortOrder.DESC);//设置按照价格降序
        searchSourceBuilder.sort("praise", SortOrder.DESC);//设置按照口碑值降序
        printResult(searchRequest);//打印搜索结果
    }


    //    public void geoShapeSearch() throws IOException{
//        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//
//        //新建geo_shape查询，设置矩形的左上角顶点和右下角顶点坐标
//        Rectangle rectangle=new Rectangle(116.457044, 39.922821,116.479466, 39.907104);
//        searchSourceBuilder.query(QueryBuilders.geoShapeQuery("location", rectangle));
//        searchRequest.source(searchSourceBuilder);//设置查询
//        printResult(searchRequest);//打印结果
//    }
    public void geoPolygonSearch() {
        SearchRequest searchRequest = new SearchRequest("hotel");//新建搜索请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //新建geo_distance查询，设置基准点坐标和周边距离
        List<GeoPoint> geoPointList = new ArrayList<GeoPoint>();//新建多边形顶点列表
        //添加多边形顶点
        geoPointList.add(new GeoPoint(39.959829, 116.417088));
        geoPointList.add(new GeoPoint(39.960272, 116.432035));
        geoPointList.add(new GeoPoint(39.965802, 116.421399));
        searchSourceBuilder.query(QueryBuilders.geoPolygonQuery("location", geoPointList));//新建geo_polygon查询
        searchRequest.source(searchSourceBuilder);//设置查询
        printResult(searchRequest);//打印结果
    }

//    public void geoDistanceSearchSort() {
//        SearchRequest searchRequest = new SearchRequest("hotel");
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.geoDistanceQuery("location").distance(10, DistanceUnit.KILOMETERS).point(40.026919, 116.47473));
//        GeoDistanceSortBuilder geoDistanceSortBuilder = SortBuilders.geoDistanceSort("location", 40.026919, 116.47473)
//                .point(40.026919, 116.47473).unit(DistanceUnit.METERS).order(SortOrder.ASC);
//        searchSourceBuilder.sort(geoDistanceSortBuilder);
//        searchRequest.source(searchSourceBuilder);
//        try {
//            SearchResponse searchResponse = client.search(searchRequest);
//            SearchHits searchHits = searchResponse.getHits();
//            for (SearchHit searchHit : searchHits) {
//                System.out.println("sv len:" + searchHit.getSortValues().length);
//                for (Object o : searchHit.getSortValues()) {
//                    BigDecimal geoDis = new BigDecimal((double) searchHit.getSortValues()[0]);
//                    System.out.println("sv:" + geoDis);
//                }
//                String index = searchHit.getIndex();
//                String id = searchHit.getId();
//                String type = searchHit.getType();
//                Long version = searchHit.getVersion();
//                Float score = searchHit.getScore();
//                String source = searchHit.getSourceAsString();
//                System.out.println("index=" + index + ",type=" + type + ",id=" + id + ",version=" + version + ",score=" + score + ",source=" + source);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//    }

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

    public long getCityCount() {
        CountRequest countRequest = new CountRequest("hotel");//客户端count请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//创建搜索builder
        searchSourceBuilder.query(new TermQueryBuilder("city", "北京"));//构建query
        countRequest.source(searchSourceBuilder);//设置查询
        try {
            CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);//执行count
            return countResponse.getCount();//返回count结果
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public List<Hotel> getHotelField(String keyword) {
        SearchRequest searchRequest = new SearchRequest("hotel");//客户端请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//创建搜索builder
        searchSourceBuilder.query(new TermQueryBuilder("city", keyword));//构建query
        searchSourceBuilder.fetchSource(new String[]{"title", "city"}, null);//设定希望返回的字段数组
        searchSourceBuilder.from(20);
        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);
        List<Hotel> resultList = new ArrayList<Hotel>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);//执行搜索
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}

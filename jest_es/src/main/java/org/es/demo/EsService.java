package org.es.demo;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class EsService {
    @Autowired
    JestClient jestClient;

    public  List<Hotel> getHotelFromTitle(String keyword){
        MatchQueryBuilder matchQueryBuilder=QueryBuilders.matchQuery("title",keyword);//match搜索
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder);
        Search search=new Search.Builder(searchSourceBuilder.toString()).addIndex("hotel").build();//创建搜索请求
        try{
            JestResult jestResult=jestClient.execute(search);//执行搜索
            if(jestResult.isSucceeded()){//判断搜索是否成功
                return jestResult.getSourceAsObjectList(Hotel.class);//将结果封装到Hotel类型的List中
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}

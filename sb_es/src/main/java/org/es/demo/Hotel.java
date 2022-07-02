package org.es.demo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "hotel")
@Data
public class Hotel {
    @Id //对应于Elasticsearch的_id
    String id;
    String title; //对应于索引中的title
    String city; //对应于索引中的city
    String price; //对应于索引中的price
}

package org.es.demo;

import lombok.Data;

@Data
public class Hotel {
    String id;//对应于文档_id
    String index;//对应于索引名称
    Float score;//对应于文档得分

    String title; //对应于索引中的title
    String city; //对应于索引中的city
    Double price; //对应于索引中的price
}

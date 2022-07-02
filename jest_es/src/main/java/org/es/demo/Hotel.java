package org.es.demo;

import lombok.Data;

@Data
public class Hotel {
    String title; //对应于索引中的title
    String city; //对应于索引中的city
    String price; //对应于索引中的price
}

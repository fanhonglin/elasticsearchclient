package org.es.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
    @Autowired
    EsService esService;
    @Autowired
    EsQrueyService esQrueyService;
    @Autowired
    EsAggService esAggService;
    @RequestMapping(value = "/test")
    public String getRec()throws  Exception{

//        esService.updateCityByQuery("hotel","北京","上海");
//        List<Hotel> hotelList=esService.getHotelField("酒店");
//        return hotelList.toString();
//        List<Hotel> hotelList=esService.getHotelFromTitle("再来");//调用Service完成搜索
//        if(hotelList!=null && hotelList.size()>0){//搜索到结果打印到前端
//            return hotelList.toString();
//        }else{
//            return "no data.";
//        }
        esAggService.getCollapseAggSearch();
        return  "hello";
    }
}

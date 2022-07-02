package org.es.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class TestController {
    @Autowired
    EsService esService;
    @RequestMapping(value = "/test")
    public String getRec()throws  Exception{
        List<Hotel> hotelList=esService.getHotelFromTitle("再来");//调用Service完成搜索
        if(hotelList!=null && hotelList.size()>0){//搜索到结果打印到前端
            return hotelList.toString();
        }else{
            return "no data.";
        }
    }
}

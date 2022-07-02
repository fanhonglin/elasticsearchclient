package org.es.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class EsService {
    @Autowired
    EsRepository esRepository;

    public List<Hotel> getHotelFromTitle(String keyword){
        return  esRepository.findByTitleLike(keyword);
    }
}

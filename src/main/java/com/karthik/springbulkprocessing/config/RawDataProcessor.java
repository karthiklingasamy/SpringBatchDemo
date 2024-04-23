package com.karthik.springbulkprocessing.config;

import com.karthik.springbulkprocessing.entity.RawData;
import org.springframework.batch.item.ItemProcessor;

public class RawDataProcessor implements ItemProcessor<RawData,RawData> {


    @Override
    public RawData process(RawData rawData) throws Exception {
        return rawData;
    }
}

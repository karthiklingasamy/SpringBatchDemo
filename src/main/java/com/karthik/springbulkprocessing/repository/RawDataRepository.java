package com.karthik.springbulkprocessing.repository;

import com.karthik.springbulkprocessing.entity.RawData;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RawDataRepository extends JpaRepository<RawData,Integer> {

}

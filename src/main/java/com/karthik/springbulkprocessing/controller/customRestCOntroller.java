package com.karthik.springbulkprocessing.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

@RestController
public class customRestCOntroller {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    private final String TEMP_STORAGE="C:\\karthik\\batch-files\\";

    @PostMapping (path="/importRawdata")
    public void importRawDataInfo2DB(@RequestParam("file")MultipartFile multipartFile) throws IOException {
        System.out.println("-----importRawDataInfo2DB");

        String originalFileName=multipartFile.getOriginalFilename();
        File fileToImport=new File(TEMP_STORAGE+originalFileName);
        multipartFile.transferTo(fileToImport);

        JobParameters jobParameters=new JobParametersBuilder()
                .addString("fullPathFileName",TEMP_STORAGE+originalFileName)
                .addLong("startAt",System.currentTimeMillis()).toJobParameters();
        try {
            System.out.println("-----inside the run method");
            jobLauncher.run(job, jobParameters);

        }catch (Exception e) {
            e.printStackTrace();
        }
        //return ResponseEntity.ok("JOB Executed Succesfully!!!");
    }

}

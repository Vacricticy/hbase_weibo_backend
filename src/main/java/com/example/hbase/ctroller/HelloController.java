package com.example.hbase.ctroller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class HelloController {
    @RequestMapping("/helloworld")
    public String HelloWorld() {
        System.out.println("success");
        return "Hello World!";
    }
}
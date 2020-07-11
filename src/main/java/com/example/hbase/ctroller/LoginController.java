package com.example.hbase.ctroller;

import com.example.hbase.entity.Dept;
import com.example.hbase.result.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;

import java.util.Objects;

@Controller
public class LoginController {

    @CrossOrigin
    @PostMapping(value = "api/login")
    @ResponseBody
    public Result login(@RequestBody Dept requestDept) {
        String row_key = requestDept.getRow_key();
        System.out.println("come in");
        return new Result(999);
    }
}


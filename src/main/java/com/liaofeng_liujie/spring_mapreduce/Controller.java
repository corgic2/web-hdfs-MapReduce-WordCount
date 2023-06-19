package com.liaofeng_liujie.spring_mapreduce;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 测试控制器
 *
 * @author: @我没有三颗心脏
 * @create: 2018-05-08-下午 16:46
 */
@RestController
public class Controller {

    @RequestMapping("/")
    public String home() {
        return "index.html";
    }
}
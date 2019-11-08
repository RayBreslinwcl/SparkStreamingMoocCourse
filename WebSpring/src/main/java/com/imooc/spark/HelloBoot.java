package com.imooc.spark;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by Administrator on 2018/9/26.
 * 第一个boot应用
 */
@RestController
public class HelloBoot {
    @RequestMapping(value="/hello",method = RequestMethod.GET)
    public String sayHello(){
        return "Hello world";
    }

    @RequestMapping(value="/first",method = RequestMethod.GET)
    public ModelAndView firstDemo(){
        return new ModelAndView("test");
    }

    @RequestMapping(value="/course_clickcount",method = RequestMethod.GET)
    public ModelAndView courseClickCountStat(){
        return new ModelAndView("demo");
    }

//    @RequestMapping(value="/course_clickcount2",method = RequestMethod.GET)
//    public ModelAndView courseClickCountStat2(){
//        return new ModelAndView("demo2");
//    }
}

package com.imooc.spark;

import com.imooc.dao.CourseClickCountDAO;
import com.imooc.dao.CourseSearchClickCountDAO;
import com.imooc.domain.CourseClickCount;
//import com.sun.tools.internal.xjc.reader.RawTypeSet;
//import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;

//import net.sf.json;
//import net.sf.json.JSONArray;
import com.imooc.domain.CourseSearchClickCount;
import net.sf.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/26.
 * web 层
 */
@RestController
public class ImoocStatApp {
    private static Map<String ,String> courses=new HashMap<>();
    static{
        courses.put("112","Spark SQL慕课网日志分析");
        courses.put("128","10小时入门大数据");
        courses.put("145","深度学习之神经网络核心原理与算法");
        courses.put("146","强大的Node.js在Web开发的应用");
        courses.put("131","vue+django实战");
        courses.put("130","Web前端性能优化");
    }

    @Autowired
    CourseClickCountDAO courseClickCountDAO;
    @Autowired
    CourseSearchClickCountDAO courseSearchClickCountDAO;
//    @RequestMapping(value="/course_clickcount_dynamic",method = RequestMethod.GET)
//   public ModelAndView courseClickCount() throws Exception{
//        ModelAndView view=new ModelAndView("index");
//        List<CourseClickCount> list = courseClickCountDAO.query("20181115");
//
////        JSONArray json=null;
//        for (CourseClickCount model:list ){
//            model.setName(courses.get(model.getName().substring(9)));
//        }
//        JSONArray json= JSONArray.fromObject(list);
//
//        view.addObject("data_json",json);
//        return  view;
//    }

    /**
     * 获得某日某各个课程的点击量
     * @return
     * @throws Exception
     * 验证：
     *
     *
     */
    @RequestMapping(value="/course_clickcount_dynamic",method = RequestMethod.POST)
    @ResponseBody
    public List<CourseClickCount> courseClickCount() throws Exception{
//    public ModelAndView courseClickCount() throws Exception{
         //自己添加修改20181126，否则总是一天的。
         SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
         String date = df.format(new Date());
         List<CourseClickCount> list = courseClickCountDAO.query("20191107");
         //        List<CourseClickCount> list = courseClickCountDAO.query(date);
         for (CourseClickCount model:list ){
         model.setName(courses.get(model.getName().substring(9)));
         }
         return  list;

//        ModelAndView view=new ModelAndView("index");
//        List<CourseClickCount> list = courseClickCountDAO.query("20191107");
//        for (CourseClickCount model:list ){
//            model.setName(courses.get(model.getName().substring(9)));
//        }
//        JSONArray json=JSONArray.fromObject(list);
//        view.addObject("data_json",json);
//        return view;
    }

     /**
     * 获得某日某个引擎引流过来的点击量
     * @param datetime 某日，格式例如：20191109
     * @param searchengine 某个搜索引擎，例如：search.yahoo.com
     * @return
     * @throws Exception
     * 验证：
    [
    {
    "name": "Spark SQL慕课网日志分析",
    "value": 2
    },
    {
    "name": "深度学习之神经网络核心原理与算法",
    "value": 2
    }
    ]
     */
    @RequestMapping(value="/course_searchclickcount_dynamic",method = RequestMethod.POST)
    @ResponseBody
    public List<CourseSearchClickCount> courseSearchClickCount(@RequestParam(value = "datetime") String datetime,@RequestParam(name = "searchengine" ) String searchengine) throws Exception{
//    public ModelAndView courseClickCount() throws Exception{
        //自己添加修改20181126，否则总是一天的。
//        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
//        String date = df.format(new Date());

//        List<CourseSearchClickCount> list = courseSearchClickCountDAO.queryDayEngineCourse("20191109","search.yahoo.com");
        List<CourseSearchClickCount> list = courseSearchClickCountDAO.queryDayEngineCourse(datetime,searchengine);
        //        List<CourseClickCount> list = courseClickCountDAO.query(date);
        for (CourseSearchClickCount model:list ){
            try {
                String name=model.getName().split("_")[2];
                model.setName(courses.get(name));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return  list;

//        ModelAndView view=new ModelAndView("index");
//        List<CourseClickCount> list = courseClickCountDAO.query("20191107");
//        for (CourseClickCount model:list ){
//            model.setName(courses.get(model.getName().substring(9)));
//        }
//        JSONArray json=JSONArray.fromObject(list);
//        view.addObject("data_json",json);
//        return view;
    }

    @RequestMapping(value = "/echarts",method = RequestMethod.GET)
    public ModelAndView echarts(){
        return new ModelAndView("echarts");
    }

    /**
     * 大屏展示
     * @return
     */
    @RequestMapping(value = "/demo_time",method = RequestMethod.GET)
    public ModelAndView echartsBig(){
        return new ModelAndView("demo_time");
    }
}

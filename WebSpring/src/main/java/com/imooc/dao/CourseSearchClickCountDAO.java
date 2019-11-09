package com.imooc.dao;

import com.imooc.domain.CourseClickCount;
import com.imooc.domain.CourseSearchClickCount;
import com.imooc.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/26.
 * 数据访问层
 */
@Component
public class CourseSearchClickCountDAO {
    /**
     * 功能1：查询某天+某一个搜索引擎：针对各个课程的引流量
     * @param day
     * @return
     * @throws Exception
     */
    public List<CourseSearchClickCount> queryDayEngineCourse(String day,String searchengine) throws Exception{
        List<CourseSearchClickCount> list =new ArrayList<>();

        //在hbase表中根据day获取实战课程对应的访问量
        Map<String ,Long> map=HBaseUtils.getInstance().query("imooc_course_search_clickcount",day+"_"+searchengine);
        for (Map.Entry<String,Long> entry:map.entrySet()){
            CourseSearchClickCount model=new CourseSearchClickCount();
            model.setName(entry.getKey());
            model.setValue(entry.getValue());

            list.add(model);
        }

        return list;
    }

    public static void main(String[] args) throws  Exception{

        CourseSearchClickCountDAO dao=new CourseSearchClickCountDAO();
        List<CourseSearchClickCount> list=dao.queryDayEngineCourse("20191109","search.yahoo.com");

        for (CourseSearchClickCount model:list){
            System.out.println(model.getName()+":"+model.getValue());
        }
    }
}

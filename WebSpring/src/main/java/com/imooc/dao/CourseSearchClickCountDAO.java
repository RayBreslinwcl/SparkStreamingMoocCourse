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
    public List<CourseSearchClickCount> query(String day) throws Exception{
        List<CourseSearchClickCount> list =new ArrayList<>();

        //在hbase表中根据day获取实战课程对应的访问量
        Map<String ,Long> map=HBaseUtils.getInstance().query("imooc_course_search_clickcount",day);
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
        List<CourseSearchClickCount> list=dao.query("20181126");

        for (CourseSearchClickCount model:list){
            System.out.println(model.getName()+":"+model.getValue());
        }
    }
}

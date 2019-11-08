package com.imooc.dao;

import com.imooc.domain.CourseClickCount;
import com.imooc.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/26.
 * 数据访问层
 */
@Component
public class CourseClickCountDAO {



    public List<CourseClickCount> query(String day) throws Exception{
        List<CourseClickCount> list =new ArrayList<>();

        //在hbase表中根据day获取实战课程对应的访问量
        Map<String ,Long> map=HBaseUtils.getInstance().query("imooc_course_clickcount",day);
        for (Map.Entry<String,Long> entry:map.entrySet()){
            CourseClickCount model=new CourseClickCount();
            model.setName(entry.getKey());
            model.setValue(entry.getValue());

            list.add(model);
        }

        return list;
    }

    public static void main(String[] args) throws  Exception{

//        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
//        String date = df.format(new Date());
//
//
//        CourseClickCountDAO dao=new CourseClickCountDAO();
//        List<CourseClickCount> list=dao.query(date);


        String date="20191107";
        CourseClickCountDAO dao=new CourseClickCountDAO();
        List<CourseClickCount> list=dao.query(date);

        for (CourseClickCount model:list){
            System.out.println(model.getName()+":"+model.getValue());
        }
    }
}

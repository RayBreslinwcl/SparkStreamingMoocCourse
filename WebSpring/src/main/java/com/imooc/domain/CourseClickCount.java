package com.imooc.domain;

import org.springframework.stereotype.Component;

/**
 * Created by Administrator on 2018/9/26.
 * 实战课程访问数量实体类
 */
@Component
public class CourseClickCount {
    private String name;
    private long value;

    public long getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(long value) {
        this.value = value;
    }
}

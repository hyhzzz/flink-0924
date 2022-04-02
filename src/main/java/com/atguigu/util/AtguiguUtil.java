package com.atguigu.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-31 12:48
 */
public class AtguiguUtil {

    public static <T> List<T> toList(Iterable<T> elements) {

        ArrayList<T> list = new ArrayList<>();


        for (T t : elements){

            list.add(t);
        }

        return list;

    }
}

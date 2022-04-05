package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-05 18:44
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordLen {

    private String word;
    private Integer len;

}

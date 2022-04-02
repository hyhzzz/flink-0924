package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-02 9:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItem {
   private Long itemId;
   private Long count;
   private Long windowEndTime;
}


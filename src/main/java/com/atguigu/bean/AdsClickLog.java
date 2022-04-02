package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-02 10:17
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdsClickLog {
   private long userId;
   private long adsId;
   private String province;
   private String city;
   private Long timestamp;
}


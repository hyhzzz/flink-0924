package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-02 10:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
   private Long userId;
   private String ip;
   private String eventType;
   private Long eventTime;
}
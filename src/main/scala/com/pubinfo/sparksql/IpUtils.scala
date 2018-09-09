package com.pubinfo.sparksql

import com.ggstar.util.ip.IpHelper

/**
 * IP解析工具类
 */
object IpUtils {


  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

}

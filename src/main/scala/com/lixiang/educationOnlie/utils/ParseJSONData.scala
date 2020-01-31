package com.lixiang.educationOnlie.utils

import com.alibaba.fastjson.{JSON, JSONObject}

object ParseJSONData {
  def getJSONData(data:String) {
    try {
      val JSONData: JSONObject = JSON.parseObject(data)
      return JSONData
    }catch {
      case ex:Exception => return null
    }
  }
}

package cn.com.wiseweb.gather

import java.net.URL
import java.util.regex.Pattern

/**
  * Created by yangguihu on 2016/10/12.
  * 根据url提取域名
  */
object DomainUtils {
  val comRe="(com.cn|com.mo|com.ph|com.pk|com.au|com.sg|co.za|ne.jp|net.cn|org.cn|gov.cn|gov.ru|lastampa.it|intoday.in|com|sg|net|cn|org|gov|cc|it|vn|co|me|tel|mobi|asia|biz|info|name|tv|hk|in|ca|tj|xin|ltd|store|vip|mom|game|lol|work|pub|club|xyz|top|ren|bid|loan|red|win|link|wang|date|party|site|online|tech|website|space|live|studio|press|news|video|click|trade|science|wiki|design|pics|photo|help|gift|rocks|band|market|software|social|lawyer|engineer|so|公司|中国|网络)$"

  def getTopDomainWithoutSubdomain(url: String,regular: String): String ={
    try {
      val host = new URL(url).getHost().toLowerCase()
      val pattern = Pattern.compile(regular)
      val matcher = pattern.matcher(host)
      if(matcher.find())
        matcher.group()
      else
        "根据规则未发现一二级域名"
    } catch {
      case ex:Exception => {
        return "一二级域名解析有误"
      }
    }
  }

  def getRE_TOP1(url: String): String ={
    //一级域名提取
    //val RE_TOP1 = "(\\w*\\.?){1}\\.(com.cn|net.cn|org.cn|gov.cn|com|net|cn|org|gov|cc|co|me|tel|mobi|asia|biz|info|name|tv|hk|公司|中国|网络)$"
    val RE_TOP1 = "(\\w*\\.?){1}\\."+comRe
    getTopDomainWithoutSubdomain(url,RE_TOP1)
  }

  def getRE_TOP2(url: String): String ={
    //一级域名提取
    //val RE_TOP2 = "(\\w*\\.?){2}\\.(com.cn|net.cn|org.cn|gov.cn|com|net|cn|org|gov|cc|co|me|tel|mobi|asia|biz|info|name|tv|hk|公司|中国|网络)$"
    val RE_TOP2 = "(\\w*\\.?){2}\\."+comRe
    getTopDomainWithoutSubdomain(url,RE_TOP2)
  }

  def main(args: Array[String]) {
    //val url = "http://www.pakistantoday.com.pk/2016/11/27/uncategorized/no-decision-yet-on-adil-gillanis-appointment-as-ambassador-to-serbia/"
    val url = "https://www.yangguihu.com";
    val topDoamin1 = getRE_TOP1(url)
    val topDoamin2 = getRE_TOP2(url)
    println(topDoamin1)
    println(topDoamin2)
  }
}

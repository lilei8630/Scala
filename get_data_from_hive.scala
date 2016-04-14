package Web_Site_Classification

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by extends_die on 16/4/13.
  */
object get_data_from_hive {


  val needExt = List("ac","co","cat","edu","net","biz","mil","int","com","gov","org","pro")

  def getDomainFromHost(host:String):String ={
    var domain = ""
    val idx1 = host.lastIndexOf('.')
    val idx2 = host.lastIndexOf('.',idx1-1)
    if(idx2 != -1){
      val check = host.substring(idx2+1,idx1)
      if(needExt.contains(check)){
        val idx3 = host.lastIndexOf('.',idx2-1)
        if (idx3 != -1){
          domain = host.substring(idx3+1)
        }else{
          domain = host
        }
      }else{
        domain = host.substring(idx2+1)
      }
    }else{
      domain = host
    }

    domain
  }

/*
* args(0) is the start_time,such as 2015120801
* args(1) is the end_time,such as 2015120802
* args(2) is the output path in hdfs,such host_query_test
* output format:host$#%^&domain$#%^&query tab(\t) query
* */
  def main(args: Array[String]): Unit = {
    val sql =
        "select a.host,a.query from (select parse_url(click.url,'HOST') as host,query " +
          "from custom.common_wap_pv lateral view explode(click_result) click_result as click " +
          "where logdate >= '"+args(0)+"' and logdate < '"+args(1)+"' and clean_state = 'OK' and page_type = 'RESULTPAGE') a " +
          "where a.host rlike '^([0-9a-zA-Z\\-\\$_\\+!\\*()]+\\.){1,6}[0-9a-zA-Z]+$'"
//    val sql =
//        select a.host,a.query from
//        (select parse_url(click.url,'HOST') as host,query
//        from custom.common_wap_pv lateral view explode(click_result) click_result as click
//        where logdate >= '2015120801' and logdate <= '2015120801'
//        and clean_state = 'OK'
//        and page_type = 'RESULTPAGE') a
//        where a.host rlike '^([0-9a-zA-Z\-\$_\+!\*()]+\.){1,6}[0-9a-zA-Z]+$'
//        """
    val conf= new SparkConf().setAppName("get_data_from_hive")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.sql(sql).rdd.filter(x=>{
      var flag = true
      if((x.get(0)==null||x.get(1)==null)){
        flag  = false
      }
      flag
    }).map(x=>x.get(0).toString+"$#%^&"+getDomainFromHost(x.get(0).toString)+"$#%^&"+x.get(1).toString+"\t"+x.get(1).toString)
    df.saveAsTextFile("host_domain_query_query_"+args(0)+"_"+args(1))
    //df.write.save("host_query_test_3.parquet")
    //sqlContext.read.parquet("")
  }
}

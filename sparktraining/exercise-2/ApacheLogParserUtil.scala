
/**
 * Created by gmedasani on 3/17/16.
 */
import java.util.regex.Pattern
import org.joda.time._

object ApacheLogParserUtil extends Serializable {

  def parseRecord(logRegex:String, logline: String):(String, String, String, DateTime, String, String, String, String,Long)= {
    val compiledPattern = Pattern.compile(logRegex)
    val matcher = compiledPattern.matcher(logline)
    if (matcher.find) {
       var size: Long = 0
       val size_field = matcher.group(9)
       if (size_field != "-"){
          size = size_field.toLong
          } else {
          size = size
       }
       (matcher.group(1),
         matcher.group(2),
         matcher.group(3),
         parse_apache_time(matcher.group(4)),
         matcher.group(5),
         matcher.group(6),
         matcher.group(7),
         matcher.group(8),
         size)
    } else {
      val date1 = new DateTime(1,1,1,1,1,1)
      (logline,"failed","",date1,"","","","",0)
    }
  }

  def parse_apache_time(s:String):DateTime= {
    val month_map = Map(("Jan",1),("Feb", 2),("Mar",3),("Apr",4),("May",5),("Jun",6),("Jul",7),("Aug",8),("Sep", 9), ("Oct",10),("Nov",11),("Dec",12))
    val datetime = new DateTime(s.slice(7,11).toInt,
                                month_map(s.slice(3,6)),
                                s.slice(0,2).toInt,
                                s.slice(12,14).toInt,
                                s.slice(15,17).toInt,
                                s.slice(18,20).toInt)
    datetime
  }
}

import org.openqa.selenium.firefox.FirefoxDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.openqa.selenium.By
import org.openqa.selenium.WebElement
import org.apache.commons.io.FileUtils
import java.lang.Thread
import java.text.SimpleDateFormat
import util.Try
import java.util.Date
import java.util.Calendar
import scala.xml.parsing.NoBindingFactoryAdapter
import org.xml.sax.InputSource
import nu.validator.htmlparser.sax.HtmlParser
import nu.validator.htmlparser.common.XmlViolationPolicy
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import java.io.File
import java.io.StringReader
import scalikejdbc._, SQLInterpolation._
 
object RaceScraper {
 
  val mail = "enter your email address"
  val password = "enter your password"
  
  def scrape() = {
    
    val driver = new HtmlUnitDriver(false)
 
    //login
    driver.get("https://account.netkeiba.com/?pid=login")
    driver.findElement(By.name("login_id")).sendKeys(mail)
    driver.findElement(By.name("pswd")).sendKeys(password+"\n")
 
    val re = """/race/(\d+)/""".r
    
    val nums =
      io.Source.fromFile("race_url.txt").getLines.toList.
    map{ s => val re(x) = re.findFirstIn(s).get; x }
 
    val urls = nums.map(s => "http://db.netkeiba.com/race/" + s)
 
    val folder = new File("html")
    if (!folder.exists()) folder.mkdir()
 
    var i = 0
 
    nums.zip(urls).map{ case (num, url) =>
      i += 1
      println(""+i+":downloading "+url)
      val file = new File(folder, num + ".html")
      if (!file.exists()) {
	driver.get(url)
	//↓ここあんまり短くしないでね！
	Thread.sleep(5000)
	val html = driver.getPageSource()
	FileUtils.writeStringToFile(file, html)
      }
    }
  }
 
}
 
object RowExtractor {

  def str2raceInfo(race_info: Array[String]): RaceInfo = {
    val DateRe(d) = race_info(8)
    RaceInfo(race_info(0), 
	 race_info(1), 
	 race_info(2).toInt, 
	 race_info(3), 
	 race_info(4), 
	 race_info(5), 
	 race_info(6).toInt, 
	 Try(race_info(7).toInt).toOption, 
	 Util.date2sqliteStr(d), 
	 race_info(9), 
	 race_info(10))
  }
 
  def str2raceResult(race_id: Int, race_result: Array[String]): RaceResult = { 
    RaceResult(race_id,
	 race_result(0),
	 race_result(1).toInt,
	 race_result(2).toInt,
	 race_result(3).split("horse/")(1).takeWhile(_ != '/'),
	 race_result(4),
	 race_result(5).toInt,
	 race_result(6).toDouble,
	 race_result(7).split("jockey/")(1).takeWhile(_ != '/'),
	 race_result(8),
	 race_result(9),
	 Try(race_result(10).toInt).toOption,
	 race_result(11),
	 Try(race_result(12).toDouble).toOption,
	 Try(race_result(13).toDouble).toOption,
	 Try(race_result(14).toInt).toOption,
	 race_result(15),
	 { val remark = race_result(18)
           if (remark.isEmpty) None else Some(remark) },
	 race_result(19),
	 race_result(20).split("trainer/")(1).takeWhile(_ != '/'),
	 race_result(21).split("owner/")(1).takeWhile(_ != '/'),
	 Try(race_result(22).toDouble).toOption
       )
  }
	
  def extract()(implicit s: DBSession) = {
 
    val files = new File("html").listFiles().reverse
 
    val entityId = """<a href="/\w+/(\d+)/"[^>]*>[^<]*</a>"""
 
    val oikiriIconLink =
      """<a href="/\?pid=horse_training&amp;id=\d+&amp;rid=\d+"><img src="/style/netkeiba.ja/image/ico_oikiri.gif" border="0" height="13" width="13"/></a>"""
 
    val commentIconLink =
      """<a href="/\?pid=horse_comment&amp;id=\d+&amp;rid=\d+"><img src="/style/netkeiba.ja/image/ico_comment.gif" border="0" height="13" width="13"/></a>"""
 
    def clean(s: String) = {
      s.replaceAll("<span>|</span>|<span/>", "").
      replaceAll(",", "").
      replaceAll(entityId, "$1").
      replaceAll(oikiriIconLink, "").
      replaceAll(commentIconLink, "")
    }
 
    val usefulFiles =
      files.
      //タイム指数が表示される2006年以降のデータだけ使う
      filter(file => FilenameUtils.getBaseName(file.getName).take(4) >= "2006").
      //以下のデータは壊れているので除外する。恐らくnetkeiba.comの不具合。
      filter(file => FilenameUtils.getBaseName(file.getName) != "200808020398").
      filter(file => FilenameUtils.getBaseName(file.getName) != "200808020399").
      toArray.
      reverse
 
    var i = 0
 
    while (i < usefulFiles.size) {
      
      val hp = new HtmlParser
  
      val saxer = new NoBindingFactoryAdapter
      hp.setContentHandler(saxer)
 
      val file = usefulFiles(i)
      i += 1

      val lines = io.Source.fromFile(file).getLines.toList
 
      val rdLines = {
	lines.
	dropWhile(!_.contains("<dl class=\"racedata fc\">")).
	takeWhile(!_.contains("</dl>"))
      }
 
      val rdHtml = rdLines.mkString + "</dl>"
      
      hp.parse(new InputSource(new StringReader(rdHtml)))
      
      val condition = saxer.rootElem.\\("span").text
      val name = saxer.rootElem.\\("h1").text
      val round = file.getName.replace(".html", "").takeRight(2)
      
      def extractDateInfo(lines: Seq[String]) = {
	lines.
        toList.
        dropWhile(!_.contains("<p class=\"smalltxt\">")).
        tail.head.replaceAll("<[^>]+>","")
      }
      
      val dateInfo = extractDateInfo(lines).trim

      val fieldScore = Try{
	val rt2Lines = {
          lines.
          dropWhile(!_.contains("class=\"result_table_02\"")).
          takeWhile(!_.contains("</table>"))
        }
 
	val rt2Html = rt2Lines.mkString + "</table>"
 
	hp.parse(new InputSource(new StringReader(rt2Html)))

	val text = saxer.rootElem.\\("td").head.text
        text.filter(_ != '?').replaceAll("\\(\\s*\\)", "").trim.toInt.toString
      }.getOrElse("")
 
      val rt1Lines = {
	lines.
	dropWhile(s => !s.contains("race_table_01")).
	takeWhile(s => !s.contains("/table"))
      }
 
      val rt1Html = rt1Lines.mkString.drop(17) + "</table>"
 
      hp.parse(new InputSource(new StringReader(rt1Html)))

      val conditions = {
        condition.
        split("/").
        map(_.replace("天候 :", "").
        replace("発走 : ", ""))
      }

      val raceInfoStr =
        (Array(name) ++
        (conditions.head.
        replaceAll(" ", "").
        replaceAll("([^\\d]+)(\\d+)m","$1,$2") +: conditions.tail) ++
        Array(round.split(" ").head, fieldScore) ++ 
        { val di = dateInfo.split(" ")
          di.take(2) :+ di.drop(2).mkString(" ") }).
        map(_.filter(_ != '?').trim).
        mkString(",").split(",")

      val raceInfo = str2raceInfo(raceInfoStr)

      RaceInfoDao.insert(raceInfo)

      val lastRowId = RaceInfoDao.lastRowId()
 
      Try{
	saxer.rootElem.\\("tr").tail 
      }.foreach{ xs =>
	xs.map(_.\\("td").map(_.child.mkString).map(clean)).
	foreach{ line =>
          val row =
	     line.map(_.replaceAll("  ", "").
                        replaceAll("<diary_snap_cut></diary_snap_cut>", "").
                        replaceAll("(牝|牡|セ)(\\d{1,2})", "$1,$2").
		        replaceAll("\\[(西|地|東|外)\\]", "$1,")).
             map(_.filter(_ != '?').trim).
             mkString(",").split(",")
	  val raceResult = str2raceResult(lastRowId, row)
	  RaceResultDao.insert(raceResult)
        }
      }
    }
  }
 
}
 
object RaceListScraper {
  
  def extractRaceList(baseUrl: String) = {
    "/race/list/\\d+/".r.findAllIn(io.Source.fromURL(baseUrl, "EUC-JP").mkString).toList.
    map("http://db.netkeiba.com" + _).
    distinct
  }
 
  def extractPrevMonth(baseList: String) = {
    "/\\?pid=[^\"]+".r.findFirstIn(
      io.Source.fromURL(baseList, "EUC-JP").
      getLines.filter(_.contains("race_calendar_rev_02.gif")).toList.head).
    map("http://db.netkeiba.com" + _).
    get
  }
 
  def extractRace(listUrl: String) = {
    "/race/\\d+/".r.findAllIn(io.Source.fromURL(listUrl, "EUC-JP").mkString).toList.
    map("http://db.netkeiba.com" + _).
    distinct
  }
 
  def scrape(period: Int) = {
    var baseUrl = "http://db.netkeiba.com/?pid=race_top"
    var i = 0
 
    while (i < period) {
      Thread.sleep(1000)
      val raceListPages =
	extractRaceList(baseUrl)
      val racePages =
	raceListPages.map{url =>
          Thread.sleep(1000)
          extractRace(url)
        }.flatten
      
      racePages.foreach{ url =>
	FileUtils.writeStringToFile(new File("race_url.txt"), url + "\n", true)
      }
 
      baseUrl = extractPrevMonth(baseUrl)
      println(i + ": collecting URLs from " + baseUrl)
      i += 1
    }
  }
 
}
 
case class RaceInfo(
  race_name: String,
  surface: String,
  distance: Int,
  weather: String,
  surface_state: String,
  race_start: String,
  race_number: Int,
  surface_score: Option[Int],
  date: String,
  place_detail: String,
  race_class: String
)

object RaceInfoDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists race_info (
  id integer primary key autoincrement,

  race_name     text    not null,
  surface       text    not null,
  distance      integer not null,
  weather       text    not null,
  surface_state text    not null,

  race_start    text    not null,
  race_number   integer not null,

  surface_score integer,
  date          text    not null,
  place_detail  text    not null,
  race_class    text    not null
);""".execute.apply

    sql"""
create index 
  date_idx
on
  race_info (date);
""".execute.apply

    sql"""
create index 
  id_date_idx 
on
  race_info (id, date);
""".execute.apply
  }

  def insert(ri: RaceInfo)(implicit s: DBSession) = {
    sql"""
insert or replace into race_info (
  race_name,
  surface,
  distance,
  weather,
  surface_state,
  race_start,
  race_number,
  surface_score,
  date,
  place_detail,
  race_class
) values (
  ${ri.race_name},
  ${ri.surface},
  ${ri.distance},
  ${ri.weather},
  ${ri.surface_state},
  ${ri.race_start},
  ${ri.race_number},
  ${ri.surface_score},
  ${ri.date},
  ${ri.place_detail},
  ${ri.race_class}
);
""".update.apply()
  }

  def lastRowId()(implicit s: DBSession): Int = {
    sql"""
select last_insert_rowid() as last_rowid
""".map(_.int("last_rowid")).single.apply().get
  }
  
  def getById(id: Int)(implicit s: DBSession): RaceInfo = {
    sql"""select * from race_info where id = ${id}""".
    map{ x => 
      RaceInfo(
        race_name = x.string("race_name"),
        surface = x.string("surface"),
        distance = x.int("distance"),
        weather = x.string("weather"),
        surface_state = x.string("surface_state"),
        race_start = x.string("race_start"),
        race_number = x.int("race_number"),
        surface_score = x.intOpt("surface_score"),
        date = x.string("date"),
        place_detail = x.string("place_detail"),
        race_class = x.string("race_class")
      )
    }.single.apply().get
  }
  
}

case class RaceResult(
  race_id: Int,
  order_of_finish: String,
  frame_number: Int,
  horse_number: Int,
  horse_id: String,
  sex: String,
  age: Int,
  basis_weight: Double,
  jockey_id: String,
  finishing_time: String,
  length: String,
  speed_figure: Option[Int],
  pass: String,
  last_phase: Option[Double],
  odds: Option[Double],
  popularity: Option[Int],
  horse_weight: String,
  remark: Option[String],
  stable: String,
  trainer_id: String,
  owner_id: String,
  earning_money: Option[Double]
)
 
object DateRe {
  
  val dateRe =
    "(\\d\\d\\d\\d)年(\\d\\d?)月(\\d\\d?)日".r
 
  def unapply(s: String) = {
    s match {
      case dateRe(y, m, d) =>
	val cal = Calendar.getInstance
        cal.set(y.toInt, m.toInt - 1, d.toInt)
	Some(new Date(cal.getTimeInMillis))
      case _ =>
        None
    }
  }
  
}
 
object RaceResultDao {

  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists race_result (
  race_id integer not null,

  order_of_finish text    not null,
  frame_number       integer not null,
  horse_number       integer not null,
  horse_id           text    not null,
  sex                text    not null,
  age                integer not null,
  basis_weight       real    not null,
  jockey_id          text    not null,
  finishing_time     text    not null,
  length             text    not null,
  speed_figure       integer,
  pass               text    not null,
  last_phase         real,
  odds               real,
  popularity         integer,
  horse_weight       text    not null,
  remark             text,
  stable             text    not null,
  trainer_id         text    not null,
  owner_id           text    not null,
  earning_money      real,
  primary key (race_id, horse_number),
  foreign key (race_id) references race_info (id)
);""".execute.apply

    sql"""
create index 
  race_id_idx 
on
  race_result (race_id);
""".execute.apply

    sql"""
create index 
  race_id_horse_id_idx 
on
  race_result (race_id, horse_id);
""".execute.apply

    sql"""
create index 
  race_id_jockey_id_idx 
on
  race_result (race_id, jockey_id);
""".execute.apply

    sql"""
create index 
  race_id_trainer_id_idx 
on
  race_result (race_id, trainer_id);
""".execute.apply

    sql"""
create index 
  race_id_owner_id_idx 
on
  race_result (race_id, owner_id);
""".execute.apply
  }
    
  def insert(rr: RaceResult)(implicit s: DBSession) = {
    sql"""
insert or replace into race_result (
  race_id,
  
  order_of_finish,
  frame_number,
  horse_number,
  horse_id,
  sex,
  age,
  basis_weight,
  jockey_id,
  finishing_time,
  length,
  speed_figure,
  pass,
  last_phase,
  odds,
  popularity,
  horse_weight,
  remark,
  stable,
  trainer_id,
  owner_id,
  earning_money
) values (
  ${rr.race_id},

  ${rr.order_of_finish},
  ${rr.frame_number},
  ${rr.horse_number},
  ${rr.horse_id},
  ${rr.sex},
  ${rr.age},
  ${rr.basis_weight},
  ${rr.jockey_id},
  ${rr.finishing_time},
  ${rr.length},
  ${rr.speed_figure},
  ${rr.pass},
  ${rr.last_phase},
  ${rr.odds},
  ${rr.popularity},
  ${rr.horse_weight},
  ${rr.remark},
  ${rr.stable},
  ${rr.trainer_id},
  ${rr.owner_id},
  ${rr.earning_money}
)
""".update.apply()
  }
  
}
 
object FeatureGenerator {
 
  def iterator()(implicit s: DBSession): Iterator[FeatureGenerator] = {
    
    val race_infos = {
      sql"select race_id, horse_number from race_result".
      map(rs => (rs.int("race_id"), rs.int("horse_number"))).
      list.
      apply
    }
      
    var count = 0
    val totalCount = race_infos.size.toDouble
 
    race_infos.
    toIterator.
    map{ case (race_id, horse_number) =>
      count += 1
      if (count % 1000 == 0)
        println("処理中 ... %7.3f％完了".format(100.0 * count / totalCount))
      new FeatureGenerator(race_id, horse_number)
    }
  }
  
}
 
class FeatureGenerator(
  val race_id: Int,
  val horse_number: Int
)(implicit s: DBSession) {
  assert(horse_number > 0)

  val RaceInfo(
    race_name,
    rawSurface,
    distance,
    rawWeather,
    surface_state,
    race_start,
    race_number,
    surface_score,
    date,
    place_detail,
    race_class
  ) = RaceInfoDao.getById(race_id)


  val grade = Util.str2cls(race_class)

  val surfaceScore = surface_score
    
  val order_of_finish =
    sql"""
select 
  order_of_finish
from
  race_result
where 
  race_id = ${race_id}
and
  horse_number = ${horse_number}""".
  map(_.string("order_of_finish")).
  single.
  apply().
  get
    
  val horse_id =
    sql"""
select
  horse_id
from
  race_result
where
  race_id = ${race_id}
and
  horse_number = ${horse_number}""".
  map(_.string("horse_id")).
  single.
  apply().
  get
  
  val jockey_id =
    sql"""
select
  jockey_id
from
  race_result
where
  race_id = ${race_id}
and
  horse_number = ${horse_number}""".
  map(_.string("jockey_id")).
  single.
  apply().
  get
  
  val trainer_id = 
    sql"""
select 
  trainer_id
from
  race_result
where
  race_id = ${race_id}
and
  horse_number = ${horse_number}""".
  map(_.string("trainer_id")).
  single.
  apply().
  get
  
  val owner_id = 
    sql"""
select
  owner_id
from
  race_result
where
  race_id = ${race_id}
and
  horse_number = ${horse_number}""".
  map(_.string("owner_id")).
  single.
  apply().
  get
    
  //Speed rating for the previous race in which the horse ran
  val preSRa = {
    sql"""
select 
  speed_figure
from 
  race_result 
inner join
  race_info 
on
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
and 
  speed_figure is not null
order by date desc
limit 1
""".map(_.int("speed_figure")).
    single.
    apply()
  }
 
  //The average of a horse’s speed rating in its last 4 races; value of zero when there is no past run
  val avgsr4 = {
    val srs =
    sql"""
select 
  speed_figure
from 
  race_result
inner join
  race_info
on
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${date}
and
  speed_figure is not null
order by date desc
limit 4
""".map(_.double("speed_figure")).
    list.
    apply()
    
    if (srs.isEmpty)
      None
    else
      Some(srs.sum / srs.size)
  }
 
  val avgWin4 = {
    val wins =
    sql"""
select
  (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
from
  race_result
inner join
  race_info
on
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${date}
and
  speed_figure is not null
order by date desc
limit 4
""".map(_.double("is_win")).
    list.
    apply()
    
    if (wins.isEmpty)
      None
    else
      Some(wins.sum / wins.size)
  }
 
  //The average speed rating of the past runs of each horse at this distance; value of zero when no previous run
  val disavesr = {
    val srs =
      sql"""
select 
  speed_figure
from 
  race_result 
inner join
  race_info 
on 
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
and 
  distance = ${distance}
and 
  speed_figure is not null
order by date desc
limit 100
""".map(_.double("speed_figure")).
    list.
    apply()
 
    if (srs.isEmpty)
      None
    else
      Some(srs.sum / srs.size)
  }
 
  val disRoc = {
    val distances =
      sql"""
select
  distance
from 
  race_result 
inner join
  race_info 
on race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.double("distance")).
    list.
    apply()
 
    if (distances.isEmpty)
      None
    else {
      val mean = distances.sum.toDouble / distances.size
      Some((distance - mean) / mean)
    }
  }
 
  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val eps = {
    val earning_money =
    sql"""
select 
  earning_money
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.doubleOpt("earning_money")).
    list.
    apply()
 
    if (earning_money.isEmpty)
      None
    else
      Some(earning_money.flatten.sum / earning_money.size)
  }
  
  //Weight carried by the horse in current race
  val weight = {
    sql"""
select
  basis_weight
from 
  race_result
where
  race_id = ${race_id}
and 
  horse_number = ${horse_number}
""".map(_.double("basis_weight")).
    single.
    apply.
    get
  }
 
  val hweight = {
    Try{
      sql"""
select 
  horse_weight
from 
  race_result
where 
  race_id = ${race_id}
and 
  horse_number = ${horse_number}
""".map(_.string("horse_weight")).
      single.
      apply.
      get.
      replaceAll("""\([^\)]+\)""", "").
      toInt
    }.toOption
  }
 
  val dhweight = {
    Try{
      sql"""
select
  horse_weight
from 
  race_result
where 
  race_id = ${race_id}
and
  horse_number = ${horse_number}
""".map(_.string("horse_weight")).
      single.
      apply.
      get.
      replaceAll(""".*\(([^\)]+)\).*""", "$1").
      toInt
    }.toOption
  }
 
  //The percentage of the races won by the horse in its career
  val winRun = {
    val wins =
      sql"""
select 
  (order_of_finish = '1') as is_win
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
limit 100
""".map(_.int("is_win")).
    list.
    apply
 
    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }
 
  //The winning percentage of the trainer in career to date of race
  val twinper = {
    val wins =
      sql"""
select 
  (order_of_finish = '1') as is_win
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  trainer_id = ${trainer_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.int("is_win")).
    list.
    apply
 
    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }
 
  //The winning percentage of the owner in career to date of race
  val owinper = {
    val wins =
      sql"""
select 
  (order_of_finish = '1') as is_win
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  owner_id = ${owner_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.int("is_win")).
    list.
    apply
 
    if (wins.isEmpty)
      None
    else
      Some(wins.sum.toDouble / wins.size)
  }
  
  //The winning percentage of the jockey in career to date of race
  val age = {
    sql"""
select 
  age
from 
  race_result
where 
  race_id = ${race_id}
and 
  horse_number = ${horse_number}
""".map(_.double("age")).
    single.
    apply.
    get
  }
 
  val dsl = {
    sql"""
select 
  (julianday(${date}) - julianday(date)) as dsl
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
and 
  speed_figure is not null
order by date desc
limit 1
""".map(_.double("dsl")).
    single.
    apply()
  }
 
  val surface = {
    Util.surface(rawSurface)
  }
 
  val weather = { 
    Util.weather(rawWeather)
  }
  
  val sex = {
    val state = sql"""
select 
  sex
from
  race_result
where
  race_id = ${race_id}
and 
  horse_number = ${horse_number}
""".map(_.string("sex")).
    single.
    apply.
    get
 
    Util.sex(state)
  }
  
  val enterTimes = {
    sql"""
select
  count(*) as count
from
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${date}
""".map(_.double("count")).
    single.
    apply.
    get
  }
 
  val odds = {
    sql"""
select 
  odds
from 
  race_result
where 
  race_id = ${race_id}
and 
  horse_number = ${horse_number}
""".map(_.doubleOpt("odds")).
    single.
    apply.
    get
  }
  
  //Total prize money earnings (finishing first, second or third) to date/Number of races entered
  val jEps = {
    val earning_money =
    sql"""
select 
  earning_money
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  jockey_id = ${jockey_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.doubleOpt("earning_money")).
    list.
    apply()
 
    if (earning_money.isEmpty)
      None
    else
      Some(earning_money.flatten.sum / earning_money.size)
  }

  val jAvgWin4 = {
    val wins =
    sql"""
select 
  (order_of_finish = '1' or order_of_finish = '2' or order_of_finish = '3') as is_win
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  jockey_id = ${jockey_id}
and 
  race_info.date < ${date}
and 
  speed_figure is not null
order by date desc
limit 4
""".map(_.double("is_win")).
    list.
    apply()
    
    if (wins.isEmpty)
      None
    else
      Some(wins.sum / wins.size)
  }

  val month = {
    date.split("-")(1) 
  }
  
  //The winning percentage of the jockey in career to date of race
  val jwinper = jWinperOf(jockey_id)

  def jWinperOf(jockey_id: String)(implicit s: DBSession): Option[Double] = {
    val wins =
      sql"""
select 
  order_of_finish = '1' as is_win
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  jockey_id = ${jockey_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.double("is_win")).
    list.
    apply()

    if (wins.nonEmpty) Some(wins.sum / wins.size)
    else None
  }

  val ridingStrongJockey = {
    val preJockeyIdOpt = sql"""
select 
  jockey_id
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 1
""".map(_.string("jockey_id")).
    single.
    apply()

    (for {
       preJockeyId <- preJockeyIdOpt
       preWinper <- jWinperOf(preJockeyId)
       winper <- jwinper
     } yield preJockeyId != jockey_id && preWinper < winper)
  }
  
  val preOOF =
    sql"""
select 
  order_of_finish
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 1
""".map(_.string("order_of_finish")).
    single.
    apply()


  val pre2OOF = {
    val orders = sql"""
select 
  order_of_finish
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 2
""".map(_.string("order_of_finish")).
    list.
    apply()

    if (orders.size == 2) orders.lastOption
    else None
  }

  val runningStyle = {
    val orders = sql"""
select 
  order_of_finish, 
  pass
from 
  race_result
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 100
""" .map(rs => (rs.string("order_of_finish"), rs.string("pass"))).
    list.
    apply

    val diff =
    orders.filter(_._1.forall(c => '0' < c && c < '9')).
    filterNot(_._2.isEmpty).
    map{ case (order_of_finish, pass) => 
      val xs = pass.split("-")
      xs.map(_.toInt).sum.toDouble / xs.size - order_of_finish.toInt }

    diff.sum / diff.size
  }
  
  val preLateStart = {
    val preRemark = sql"""
select 
  remark
from 
  race_result 
inner join 
  race_info 
on 
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 1
""".map(_.stringOpt("remark")).
    single.
    apply().
    flatten

    preRemark.nonEmpty && preRemark.get == "出遅れ"
  }

  val lateStartPer = {
    val lateList = sql"""
select 
  (remark = '出遅れ') as is_late
from 
  race_result
inner join
  race_info
on
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${date}
order by date desc
limit 100
""".map(_.intOpt("is_late").getOrElse(0)).
    list.
    apply()
    
    lateList.sum.toDouble / lateList.size
  }

  val preLastPhase = {
    sql"""
select 
  last_phase
from 
  race_result 
inner join 
  race_info
on 
  race_result.race_id = race_info.id
where
  horse_id = ${horse_id}
and
  race_info.date < ${date}
order by date desc
limit 1
""".map(_.doubleOpt("last_phase")).
    single.
    apply().
    flatten    
  }


  val course = {
    Util.course(rawSurface)
  }

  val placeCode = {
    sql"""
select 
  place_detail
from 
  race_info
where
  id = ${race_id}
""".map(_.string("place_detail")).
    single.
    apply.
    get.
    replaceAll("\\d+回([^\\d]+)\\d+日目", "$1")
  }

  val headCount = {
    sql"""
select
  count(*) as head_count
from
  race_result
where
  race_id = ${race_id}
""".map(_.double("head_count")).
    single.
    apply.
    get
  }
  
  private val preRaceIdOpt = sql"""
select 
  race_id
from 
  race_result 
inner join
  race_info 
on
  race_result.race_id = race_info.id
where 
  horse_id = ${horse_id}
and 
  race_info.date < ${date}
order by date desc
limit 1
""".map(_.int("race_id")).
    single.
    apply()

  val preHeadCount = {
    for { pre_race_id <- preRaceIdOpt } yield {
      sql"""
select
  count(*) as head_count
from
  race_result
where
  race_id = ${pre_race_id}
""".map(_.double("head_count")).
      single.
      apply.
      get
    }
  }
  
  val surfaceChanged = {
    for { pre_race_id <- preRaceIdOpt } yield {
      val info = RaceInfoDao.getById(pre_race_id)
      surface != Util.surface(info.surface)
    }
  }
  
}
 
object FeatureDao {
 
  def createTable()(implicit s: DBSession) = {
    sql"""
create table if not exists feature (
  race_id integer not null,
  horse_number  integer not null,

  grade integer not null,
  order_of_finish integer,
  
  horse_id text not null,
  jockey_id text not null,
  trainer_id text not null,

  age real,
  avgsr4 real,
  avgWin4 real,
  dhweight real,
  disavesr real,
  disRoc real,
  distance real,
  dsl real,
  enterTimes real,
  eps real,
  hweight real,
  jwinper real,
  odds real,
  owinper real,
  preSRa real,
  sex text,
  surface text,
  surfaceScore real,
  twinper real,
  weather text,
  weight real,
  winRun real,

  jEps real,
  jAvgWin4 real,
  preOOF real,
  pre2OOF real,

  month real,
  ridingStrongJockey real,
  runningStyle real,
  preLateStart real,
  preLastPhase real,
  lateStartPer real,
  course text, 
  placeCode text,

  headCount real,
  preHeadCount real,

  primary key (race_id, horse_number)
);
""".execute.apply
  }
 
  def insert(fg: FeatureGenerator)(implicit s: DBSession) = {
    import Util.position2cls
    sql"""
insert or replace into feature (
  race_id,
  horse_number,
  grade,
  order_of_finish,
  
  horse_id,
  jockey_id,
  trainer_id,

  age,
  avgsr4,
  avgWin4,
  dhweight,
  disavesr,
  disRoc,
  distance,
  dsl,
  enterTimes,
  eps,
  hweight,
  jwinper,
  odds,
  owinper,
  preSRa,
  sex,
  surface,
  surfaceScore,
  twinper,
  weather,
  weight,
  winRun,

  jEps,
  jAvgWin4,
  preOOF,
  pre2OOF,

  month,
  ridingStrongJockey,
  runningStyle,
  preLateStart,
  preLastPhase,
  lateStartPer,
  course,
  placeCode,
  
  headCount,
  preHeadCount

) values (
  ${fg.race_id},
  ${fg.horse_number},
  ${fg.grade},
  ${position2cls(fg.order_of_finish)._1},
  
  ${fg.horse_id},
  ${fg.jockey_id},
  ${fg.trainer_id},

  ${fg.age},
  ${fg.avgsr4},
  ${fg.avgWin4},
  ${fg.dhweight},
  ${fg.disavesr},
  ${fg.disRoc},
  ${fg.distance},
  ${fg.dsl},
  ${fg.enterTimes},
  ${fg.eps},
  ${fg.hweight},
  ${fg.jwinper},
  ${fg.odds},
  ${fg.owinper},
  ${fg.preSRa},
  ${fg.sex},
  ${fg.surface},
  ${fg.surfaceScore},
  ${fg.twinper},
  ${fg.weather},
  ${fg.weight},
  ${fg.winRun},
  ${fg.jEps},
  ${fg.jAvgWin4},
  ${fg.preOOF},

  ${fg.pre2OOF},
  ${fg.month},
  ${fg.ridingStrongJockey},
  ${fg.runningStyle},
  ${fg.preLateStart},
  ${fg.preLastPhase},
  ${fg.lateStartPer},
  ${fg.course}, 

  ${fg.placeCode},
  
  ${fg.headCount},
  ${fg.preHeadCount}
  
)
""".update.apply
  }
 
  def rr2f() = {
    DB.readOnly{ implicit s =>
      val fgs = 
        FeatureGenerator.iterator()
      fgs.grouped(1000).foreach{ fgs =>
        //1000回インサートする度にコミットする
        DB.localTx{ implicit s =>
          fgs.foreach(FeatureDao.insert)
        }
      }
    }
  }
 
}
 
object Util {
 
//weatherの種類
//List(晴, 曇, 雨, 小雨, 雪)
 
//surfaceの種類
//List(芝右, ダ右, 障芝, 芝右 外, 障芝 ダート, ダ左, 芝左, 障芝 外, 芝左 外, 芝直線, 障芝 外-内, 障芝 内-外, 芝右 内2周)
 
//sexの種類
//List(牡, 牝, セ)
 
  private val weatherState =
    Seq("晴", "曇", "雨", "小雨", "雪")
  
  def weather(s: String): String = {
    weatherState.map{ state =>
      if (s == state) return state
    }
    return "他"
  }
 
  private val surfaceState =
    Seq("芝", "ダ")
 
  def surface(s: String): String = {
    surfaceState.foreach { state =>
      if (s.contains(state)) return state
    }
    return "他"
  }
 
  private val sexState =
    Seq("牡", "牝", "セ")
 
  def sex(s: String): String = {
    sexState.map{ state =>
      if (s == state) return state
    }
    return "他"
  }

  private val courseState =
    Seq("直線", "右","左","外")

  def course(s: String): String = {
    courseState.map{ state =>
      if (s.contains(state)) return state
    }
    return "他"
  }

  /**
   * Dateをyyyy-MM-DDの形式の文字列に変換
   */
  def date2sqliteStr(date: Date): String = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    "%04d-%02d-%02d".format(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH)+1,
      cal.get(Calendar.DAY_OF_MONTH))
  }
 
  val str2clsMap =
    Array(
      "オープン",
      "1600万下",
      "1000万下",
      "500万下",
      "未勝利",
      "新馬").zipWithIndex
 
  def str2cls(s: String): Int = {
    str2clsMap.foreach{ case (a, b) =>
      if (s.contains(a)) return b }
    sys.error("class not found:"+s)
  }
 
  val positionState =
    Array(
      "(降)",
      "(再)",
      "中",
      "取",
      "失",
      "除"
    ).zipWithIndex
 
  def position2cls(s: String): (Option[Int], Option[Int]) = {
    positionState.foreach{ case (a, b) =>
      if (s.contains(a))
        return (Try(s.replace(a, "").toInt).toOption, Some(b))
    }
    (Try(s.replaceAll("[^\\d]", "").toInt).toOption, None)
  }
 
  val surfaceStates =
    Array(
      "ダート : 稍重",
      "ダート : 重",
      "ダート : 良",
      "ダート : 不良",
      "芝 : 良",
      "芝 : 稍重",
      "芝 : 重",
      "芝 : 不良")
  
  val startTimeFormat = new SimpleDateFormat("hh:mm")
  val finishingTimeFormat = new SimpleDateFormat("m:ss.S")
  
  val example =
    Array[Any](
  //race_name
  "3歳上500万下",
  //surface
  "芝右",
  //distance
  2600,
  //weather
  "晴",
  //surface
  "芝 : 良",
  //race start
  startTimeFormat.parse("16:15"),
  //race number
  12,
  //surface score
  -4,
  //date
  "2014年9月7日",
  //place_detail
  "2回小倉12日目",
  //class
  "サラ系3歳以上500万下○混○特指(定量)",
  //finish position
  1,
  //frame number
  7,
  //horse number
  14,
  //horse id
  "2011100417",
  //sex
  "牡",
  //age
  3,
  //basis weight
  54,
  //jockey id
  "01115",
  //finishing time
  finishingTimeFormat.parse("2:39.7"),
  //length
  "",
  //speed figure
  98,
  //pass
  "15-14-11-7",
  //
  35.5,
  //odds
  3.5,
  //integer popularity not null
  1,
  //text horse weight
  "466(+4)",
  //text
  "",
  //text 
  "",
  //text remark
  "出遅れ",
  //text stable not null
  "西",
  //text trainer_id not null
  "01071",
  //text owner_id not null
  "708800",
  //real earning_money
  730.0
    )
 
}

object Main {

  def init() = {
    Class.forName("org.sqlite.JDBC")
    ConnectionPool.singleton("jdbc:sqlite:race.db", null, null)
  }

  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("collecturl") => 
        //過去10年分のURLを収集する
        RaceListScraper.scrape(period = 12 * 10)
      case Some("scrapehtml") => 
        RaceScraper.scrape()
      case Some("extract") => 
        init()
        DB.localTx { implicit s =>
          RaceInfoDao.createTable()
          RaceResultDao.createTable()
          RowExtractor.extract()
        }
      case Some("genfeature") =>
        GlobalSettings.loggingSQLAndTime = new LoggingSQLAndTimeSettings(
          enabled = false,
          logLevel = 'info
        )
        init()
        DB.localTx { implicit s =>
          FeatureDao.createTable()
        }
        FeatureDao.rr2f()
      case _ =>
        sys.error("invalid argument")
    }
  }
}

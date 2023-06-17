package Jobs

import org.apache.spark.sql.SparkSession

object Master1 extends App {
  val Job = "Job_master"

  val spark = SparkSession
    .builder()
    .appName(Job)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val vente = spark.read.textFile("C:\\Users\\WILLIAMS KOFFI\\Desktop\\sample_ventes.txt")
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd=spark.sparkContext.parallelize(dataSeq)
  vente.foreach(f=>{ println(f) })
  val rdd2 = rdd.map(f => { (f._2, (f._1,f._2))} )
  val rdd3 = rdd.sortByKey(false, 6)
  val rdd4 = rdd.reduce( (a,b)=> ("min",a._2 min b._2))._2
  val data = Seq(("Project", 1), ("Gutenberg’s", 1), ("Alice’s", 1), ("Adventures", 1), ("in", 1), ("Wonderland", 1), ("Project", 1), ("Gutenberg’s", 1), ("Adventures", 1), ("in", 1), ("Wonderland", 1), ("Project", 1), ("Gutenberg’s", 1))
  val columns = Seq("language","users_count")

  rdd2.foreach(println)
  println("Nombre initial de partitions :"+ rdd.getNumPartitions)
  println("Collect :"+ rdd.collect())
  println("Premier element :"+ rdd.first())
  println("Williams is beautiful !")

}

package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/**
  * Do you think that partitioning your data would help?
  * Have you thought about persisting some of your data?
  * Can you think of why persisting your data in memory may be helpful for this algorithm?
  * Of the non-empty clusters, how many clusters have "Java" as their label
  * (based on the majority of questions, see above)? Why?
  * Only considering the "Java clusters", which clusters stand out and why?
  * How are the "C# clusters" different compared to the "Java clusters"?
  */

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /**
    * Load postings from the given file
    <postTypeId>:     Type of the post. Type 1 = question,
                      type 2 = answer.

    <id>:             Unique id of the post (regardless of type).

    <acceptedAnswer>: Id of the accepted answer post. This
                      information is optional, so maybe be missing
                      indicated by an empty string.

    <parentId>:       For an answer: id of the corresponding
                      question. For a question:missing, indicated
                      by an empty string.

    <score>:          The StackOverflow score (based on user
                      votes).

    <tag>:            The tag indicates the programming language
                      that the post is about, in case it's a
                      question, or missing in case it's an answer.
    */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /**
    * Ideally, we want to obtain an RDD with the pairs of (𝚀𝚞𝚎𝚜𝚝𝚒𝚘𝚗, 𝙸𝚝𝚎𝚛𝚊𝚋𝚕𝚎[𝙰𝚗𝚜𝚠𝚎𝚛]).
    * However, grouping on the question directly is expensive (can you imagine why?),
    * so a better alternative is to match on the QID,
    * thus producing an 𝚁𝙳𝙳[(𝚀𝙸𝙳, 𝙸𝚝𝚎𝚛𝚊𝚋𝚕𝚎[(𝚀𝚞𝚎𝚜𝚝𝚒𝚘𝚗, 𝙰𝚗𝚜𝚠𝚎𝚛))].
    *
    * To obtain this, in the 𝚐𝚛𝚘𝚞𝚙𝚎𝚍𝙿𝚘𝚜𝚝𝚒𝚗𝚐𝚜 method, first filter the questions and answers
    * separately and then prepare them for a join operation by extracting the QID value
    * in the first element of a tuple. Then, use one of the 𝚓𝚘𝚒𝚗 operations (which one?)
    * to obtain an 𝚁𝙳𝙳[(𝚀𝙸𝙳, (𝚀𝚞𝚎𝚜𝚝𝚒𝚘𝚗, 𝙰𝚗𝚜𝚠𝚎𝚛))]. Then, the last step is to obtain
    * an 𝚁𝙳𝙳[(𝚀𝙸𝙳, 𝙸𝚝𝚎𝚛𝚊𝚋𝚕𝚎[(𝚀𝚞𝚎𝚜𝚝𝚒𝚘𝚗, 𝙰𝚗𝚜𝚠𝚎𝚛)])]. How can you do that, what method do
    * you use to group by the key of a pair RDD?
    * Finally, in the description we made QID, Question and Answer separate types,
    * but in the implementation QID is an 𝙸𝚗𝚝 and both questions and answers are of type 𝙿𝚘𝚜𝚝𝚒𝚗𝚐.
    * Therefore, the signature of 𝚐𝚛𝚘𝚞𝚙𝚎𝚍𝙿𝚘𝚜𝚝𝚒𝚗𝚐𝚜 is:

    def groupedPostings(postings: RDD[/* Question or Answer */ Posting]):
        RDD[(/*QID*/ Int, Iterable[(/*Question*/ Posting, /*Answer*/ Posting)])]

    * Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    val questions = postings.filter(posting => posting.postingType == 1).map(posting => (posting.id, posting))
    val answers = postings.filter(posting => posting.postingType == 2).map(posting => (posting.parentId.get, posting))

    val qaPair = questions.join(answers)
    qaPair.groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    // (question, high score)
    grouped.map(qaPair => (qaPair._2.head._1, answerHighScore(qaPair._2.map(x => x._2).toArray)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    // (language, score)
    scored.map(score => (firstLangInTag(score._1.tags, langs).get * langSpread, score._2))
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val closest = vectors.cache().groupBy(vector => findClosest(vector, means)).mapValues(averageVectors).collect()
    val newMeans = means.clone()

    for {
      mean <- closest
    } newMeans(mean._1) = mean._2

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      // vs = Iterable[(language, score)]
      // most common language in the cluster
      val mostCommonLanguage = vs.groupBy(x => x._1).maxBy(x => x._2.size)
      val langLabel: String   = langs(mostCommonLanguage._1 / langSpread)
      val langPercent: Double = 100.0 * mostCommonLanguage._2.size / vs.size // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val orderedVs = vs.toList.sortBy(x => x._2)
      val medianScore: Int    =
        if (vs.size%2 != 0) orderedVs(vs.size/2)._2
        else (orderedVs(vs.size/2)._2 + orderedVs(vs.size/2 - 1)._2) / 2

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}

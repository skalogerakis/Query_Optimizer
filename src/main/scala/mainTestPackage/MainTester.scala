package mainTestPackage

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions.{col, column, countDistinct, hash, md5}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}
//import org.apache.log4j.{Level, LogManager}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
import scala.collection.mutable.HashMap


object MainTester {

  /**
   *  A function that implements the main Query Optimization logic.
   * @param fullMap: Hashmap with (keys,values). Keys are the statements in AS statements and values are a list with all the possible tabs for join
   * @param costMap: Hashmap with keys the tab names and values the corresponding join cost
   * @return: String with the FULLY OPTIMIZED WHERE statement
   */
  def QueryOptimizerLogic(fullMap: HashMap[String, Array[String]], costMap: HashMap[String, Double]): String ={


    /**
     * We could consider this problems as a graph problem, with the tables being the vertices and their joins as edges.
     * Essentially we want to find the Hamiltonian path https://www.hackerearth.com/practice/algorithms/graphs/hamiltonian-path/tutorial/
     * We also know that when there are N nodes, the minimum number of edges in a connected graph are n-1.
     * PROOF: https://www.quora.com/How-do-I-prove-that-the-minimum-number-of-edges-in-a-connected-graph-with-n-vertices-is-n-1
     * The final goal to achieve the best joins possible(with the least cost).
     */

    var bestComb : List[(String, Double, String, String)] = Nil // A list to preserve the best combinations


    //First iterate over the Hashmap (key,values). Values is a list that contains all the available tables between them
    //We are certain that there are no joins where only one table of a specific key exists
    for (dictIter <- fullMap; if dictIter._2.length > 1 ){

      var tempKeyComb : List[(String, Double, String, String)] = Nil  //List with all the combinations for table joins in each key

      for(innerList <- dictIter._2.indices){  //  Iterate over the list elements. innerList is an index value

        for(remElem <- innerList+1 until dictIter._2.length){   // Go from the next index to the end of the table to track all combinations
          val tempProd: Double = costMap(dictIter._2(innerList)) * costMap(dictIter._2(remElem))  // Find the cost of join between two arrays, by calculating the product
          val tempTuple: (String, Double, String, String) = (dictIter._2(innerList)+"."+dictIter._1+"="+dictIter._2(remElem)+"."+dictIter._1, tempProd, dictIter._2(innerList), dictIter._2(remElem))  // Create a temporary tuple with the where join statement and the cost

          tempKeyComb = tempTuple :: tempKeyComb
        }
      }

      var bestCombTables : List[String] = Nil // A list to preserve the best combinations
      val sortedtempComb : List[(String, Double, String, String)]= tempKeyComb.sortBy(_._2)  //Sort everything by the join cost(ascending)

      /*
          Keep the N - 1 elements that we want from each case, as stated in the beginning. NOTE N-1 from the initial list with the elements
       */

      var elemIter = 0
      var counter = 0
      while(elemIter < dictIter._2.length - 1){

        //If there is an element that already joins two tables that are joined move to the next element
        if(bestCombTables.contains(sortedtempComb(counter)._3) && bestCombTables.contains(sortedtempComb(counter)._4)){
          counter += 1
        }else{

          if (!bestCombTables.contains(sortedtempComb(counter)._3)){ //If an element does not exist in the tables already joined add it
            bestCombTables = sortedtempComb(counter)._3 :: bestCombTables
          }

          if(!bestCombTables.contains(sortedtempComb(counter)._4)){
            bestCombTables = sortedtempComb(counter)._4 :: bestCombTables
          }

          bestComb = sortedtempComb(counter) :: bestComb //If we reach here it means that at least one of the tables in the join is new
          elemIter += 1
          counter += 1
        }
      }

    }

    val sortedBestComb : List[String] = bestComb.sortBy(_._2).map(x => x._1) //Sort once again to get the final ordering, and keep the only the combinations
    val finalWhereQuery: String = " WHERE " +sortedBestComb.mkString(" AND ") //Final WHERE STATEMENT OPTIMIZED

    //println("Final WHERE STATEMENT "+ finalWhereQuery) //WHERE sanity check
    finalWhereQuery
  }

  def QueryOptimizer(inputQuery: String, fileStatisticsPath: String) : String = {

    val newlineSplit: Array[String] = inputQuery.split("\\r?\\n")    //Split the input based on the newline character

    if (newlineSplit(newlineSplit.length - 1).split(" ")(1).toInt == 1 ) return inputQuery

    val query: String = newlineSplit(1) //Essentially we want to modify just the query(2nd entry), while the rest remain the same
    val _SelectQuery: String = StringUtils.substringBefore(query, "FROM") // Select statement will not change in the output

    //Extracts contents of the query from the first FROM statement until the last WHERE STATEMENT
    var initQueryDiv : String = StringUtils.substringAfter(query, "FROM")
    initQueryDiv = StringUtils.substringBeforeLast(initQueryDiv, "WHERE").replaceAll("\n", " ").trim

    //Regex to split string by commas that is not included inside the parentheses to get all the different tabs
    val tabSplitter: Array[String] = initQueryDiv.split(",\\s*(?![^()]*\\))")

    // Structure that contains an Array[(tab name, AS statements, full table name ,whole from statement)]
    val tableIdentifier: Array[(String, String, String, String)] = tabSplitter
      .map(x=>(StringUtils.substringAfterLast(x, "AS").trim, StringUtils.substringBetween(x, "SELECT", "FROM"),  if (StringUtils.substringAfter(x, "FROM").trim.split(" ")(0).endsWith(")")) StringUtils.substringAfter(x, "FROM").trim.split(" ")(0).dropRight(1) else StringUtils.substringAfter(x, "FROM").trim.split(" ")(0),x))

    //costMap is a Hashmap containing the tab name and the tuple count.
    //  Implementation reading statistics from file
    val inputMap:Map[String,Double] = scala.io.Source.fromFile(fileStatisticsPath).getLines.map {
      l =>
        l.split(',') match {
          case Array(k, v, _*) => "table" + k.replace("-", "_").replace("=", "_E_").trim -> v.toDouble
          case _ => "-1.0" -> -1.0    //this is to get when there is an empty line that causes an exception otherwise
        }
    }.toMap.filter(x=> x._2 != -1.0 )  //Keep only the valid entries


    var costMap: HashMap[String, Double] = new HashMap()
    tableIdentifier.map(n=> costMap.update(n._1, inputMap(n._3)))  //TODO maybe do something special when the stat does not exist from the file


    val finalTable: Array[(String,Double)] = tableIdentifier.map(t => (t._4, costMap.get(t._1) match {
      case Some(value) => value
    })).sortBy(_._2)    //Final sorted statements for the FROM statement

    val _FromQuery: String = "FROM "+finalTable.map(x => x._1).mkString(", ") //The final FROM part of the query

    val rgxAS = "AS\\s((\\w+))|as\\s(\\w+)".r //Regex to extract statements that start with AS or as
    var fullMap: HashMap[String, Array[String]] = new HashMap() //A hashmap with keys the statements stored in AS statements and values all the possible tabs for join

    tableIdentifier.foreach( x => {
      rgxAS.findAllIn(x._2).matchData.foreach(

        m => fullMap.get(m.group(1)) match {
          case None => fullMap.update(m.group(1), Array(x._1))  //In case the element does not exist initialize the array
          case Some(value) => fullMap.update(m.group(1), value :+ x._1) //Otherwise append to the list the new element
        }
      )
    })

    val _WhereQuery: String = QueryOptimizerLogic(fullMap, costMap) //Function that returns the optimized WHERE statement
    val _Query: String = _SelectQuery + _FromQuery + _WhereQuery

    newlineSplit(1) = _Query  //Change only the final query
    val finalOutput: String = newlineSplit.mkString("\n") //Merge once again the initially split string, with the newline character

    return finalOutput
  }


  def builtInOptimizer() : Unit= {
    val sparkSession = SparkSession.builder()
      .appName("Spark Query Optimizer")
      .master("local[*]")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.joinReorder.enabled", "true")
      .config("spark.sql.cbo.joinReorder.dp.star.filter", "true")
      .config("spark.sql.statistics.histogram.enabled", "true")
      .config("spark.sql.statistics.histogram.numBins", 20)
      //      .config("spark.sql.warehouse.dir", "file:///home/skalogerakis/Documents/Workspace/CS460_Bonus/test")
      //      .enableHiveSupport()
//      .config("spark.sql.adaptive.enabled","true")
//      .config("spark.sql.adaptive.skewJoin.enabled","true")
      .getOrCreate()

    //This hides too much log information and sets log level to error
    sparkSession.sparkContext.setLogLevel("ERROR")

    //Define using sql queries
    //    println(sparkSession.sql("select * from parquet.`/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_`").count())

    //    println(sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_").count())

    val emailTab = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_").cache()
    val nameTab = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_name_").cache()
    val telephoneTab = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_telephone_").cache()
    val worksForTab = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_worksfor_").cache()
    val typeTab = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/Query4_Partitions/_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_").cache()

    println(s"COUNT FOR -> EmailAddre: ${emailTab.count()}, Name: ${nameTab.count()} , Telephone: ${telephoneTab.count()} , worksFor: ${worksForTab.count()}, Type: ${typeTab.count()}")

    //    emailTab.show(10,false)
    nameTab.show(10,false)
    //    telephoneTab.show(10,false)
    //    worksForTab.show(10,false)
    //    typeTab.show(10,false)


    import sparkSession.implicits._

    //Before applying the hash function to a column
    println(s"Distinct Before Hashing -> ${nameTab.select("_2").distinct.count}")
    nameTab.sort("_2").show(10)

    //After applying the hash function
    val hashednameTab = nameTab.withColumn("hash", functions.hash($"_2")).withColumn("hash1", functions.hash($"_1")) //TODO also md5 function however returns hex
    println(s"Distinct After Hashing ${hashednameTab.select("hash").distinct.count}")
    hashednameTab.show(10,false)


//    import org.apache.spark.sql.catalyst.TableIdentifier
//    val sessionCatalog = sparkSession.sessionState.catalog

    val tableName = "name"

    import org.apache.spark.sql.catalyst.TableIdentifier
    val tableId = TableIdentifier(tableName)

    val sessionCatalog = sparkSession.sessionState.catalog
    sessionCatalog.dropTable(tableId, ignoreIfNotExists = true, purge = true)


    //    hashednameTab.write.option("path",s"/home/skalogerakis/Documents/Workspace/CS460_Bonus/test/${tableName}").saveAsTable(tableName)
    //    hashednameTab.createOrReplaceTempView(tableName)
    //    sparkSession.sqlContext.cacheTable(tableName)


    //    val sqlContext = new HiveContext(sparkSession)
//    sparkSession.sqlContext.sql(s"LOAD DATA LOCAL INPATH 'file:///home/skalogerakis/Documents/Workspace/CS460_Bonus/test' INTO TABLE ${tableName}")

//    val tst = sparkSession.read.parquet("/home/skalogerakis/Documents/Workspace/CS460_Bonus/test")
//    tst.printSchema()
//
//
//    sparkSession.sqlContext.sql("show tables").show()
//
//      sparkSession.sqlContext.sql(s"REFRESH TABLE $tableName")
//    val df = sparkSession.sqlContext.table(tableName)
//    val allCols = df.columns.mkString(",")
//    val analyzeTableSQL = s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS $allCols"   //FOR ALL COLUMNS SEEMS TO WORK
//    sparkSession.sqlContext.sql(analyzeTableSQL)
//
//    println("DESCRIPTION FOR NON-HASH")
//    val defdescExtSQL = s"DESC EXTENDED $tableName _2"
//    sparkSession.sqlContext.sql(defdescExtSQL).show(truncate = false)
//
//    println("DESCRIPTION FOR HASH")
//    val descExtSQL = s"DESC EXTENDED $tableName hash" //or just emit hash column name to get full statistics
//    sparkSession.sqlContext.sql(descExtSQL).show(truncate = false)
  }

  def main(args: Array[String]): Unit = {

//    val inputQuery = ">>>>> Q14.txt\nX SELECT tab0.X AS X FROM (SELECT s AS X FROM table00003__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent>') AS tab0\npartitions 00003-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_\nTP 1"
//    val inputQuery = ">>>>> Q2.txt\nX SELECT tab5.Y AS Y,tab2.Z AS Z,tab3.X AS X FROM (SELECT s AS Y FROM table00001__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>') AS tab1, (SELECT s AS Z, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_suborganizationof_) AS tab4, (SELECT s AS X, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_undergraduatedegreefrom_) AS tab5, (SELECT s AS X FROM table00002__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>') AS tab0, (SELECT s AS X, o AS Z FROM table00004__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_memberof_) AS tab3, (SELECT s AS Z FROM table00004__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>') AS tab2 WHERE tab1.Y=tab4.Y AND tab4.Y=tab5.Y AND tab0.X=tab3.X AND tab3.X=tab5.X AND tab2.Z=tab3.Z AND tab3.Z=tab4.Z \npartitions 00002-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00001-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_undergraduatedegreefrom_,00001-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00004-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00004-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_memberof_,00001-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_suborganizationof_\nTP 6"
//    val inputQuery = ">>>>> Q4.txt\nX SELECT tab4.X AS X,tab2.Y1 AS Y1,tab3.Y2 AS Y2,tab4.Y3 AS Y3 FROM (SELECT s AS X FROM table00001__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor>') AS tab0, (SELECT s AS X FROM table00001__3_E__http___swat_cse_lehigh_edu_onto_univ_bench_owl_worksfor_ WHERE o == '<http://www.Department0.University0.edu>') AS tab1, (SELECT s AS X, o AS Y1 FROM table00001__3_E__http___swat_cse_lehigh_edu_onto_univ_bench_owl_name_) AS tab2, (SELECT s AS X, o AS Y2 FROM table00001__3_E__http___swat_cse_lehigh_edu_onto_univ_bench_owl_emailaddress_) AS tab3, (SELECT s AS X, o AS Y3 FROM table00001__3_E__http___swat_cse_lehigh_edu_onto_univ_bench_owl_telephone_) AS tab4 WHERE tab0.X=tab1.X AND tab1.X=tab2.X AND tab2.X=tab3.X AND tab3.X=tab4.X \npartitions 00001-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00001-_3=_http___swat_cse_lehigh_edu_onto_univ_bench_owl_worksfor_,00001-_3=_http___swat_cse_lehigh_edu_onto_univ_bench_owl_name_,00001-_3=_http___swat_cse_lehigh_edu_onto_univ_bench_owl_emailaddress_,00001-_3=_http___swat_cse_lehigh_edu_onto_univ_bench_owl_telephone_\nTP 5"

//    TODO uncomment that
//    val inputQuery = ">>>>> Q4.txt\nX SELECT tab4.X AS X,tab2.Y1 AS Y1,tab3.Y2 AS Y2,tab4.Y3 AS Y3 FROM (SELECT s AS X, o AS Y2 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_) AS tab3, (SELECT s AS X, o AS Y3 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_telephone_) AS tab4, (SELECT s AS X FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_worksfor_ WHERE o == '<http://www.Department0.University0.edu>') AS tab1, (SELECT s AS X, o AS Y1 FROM table00003__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_name_) AS tab2, (SELECT s AS X FROM table00003__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>') AS tab0 WHERE tab3.X=tab4.X AND tab1.X=tab4.X AND tab2.X=tab4.X AND tab0.X=tab4.X\npartitions 00003-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00003-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_worksfor_,00003-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_name_,00003-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_emailaddress_,00003-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_telephone_\nTP 5"
//    println("Initial input is: \n\n"+inputQuery)
//
//    val fileStatisticsPath = "/home/skalogerakis/Documents/Workspace/CS460_Bonus/Data/Query4/statTables.txt"
//    val finOutput = QueryOptimizer(inputQuery, fileStatisticsPath)
//    println("\n\nOutput is: \n\n"+finOutput)

    builtInOptimizer()




  }

}




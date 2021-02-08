package algorithm.bigclam



import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.io.Source
import scala.math.abs
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry,IndexedRow,IndexedRowMatrix,BlockMatrix}
import scala.util.control.Breaks._
import org.apache.spark.mllib.linalg.{Vectors,Vector,DenseVector,SparseVector,Matrix,Matrices}
import scala.util.Random
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.broadcast.Broadcast
import com.clearspring.analytics.hash.MurmurHash
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.hadoop.fs.Path



class BigClam extends org.apache.spark.internal.Logging with Serializable {

def loglikelihood(F:DataFrame,sumF:breeze.linalg.DenseMatrix[Double],alledges:DataFrame,kvalue:Int,spark: SparkSession):Double ={
    import spark.implicits._
    val MIN_P_ = 0.0001
    val MAX_P_ = 0.9999
     def aggregatorNeighVec(a: Array[org.apache.spark.mllib.linalg.Vector], b: Array[org.apache.spark.mllib.linalg.Vector]): Array[org.apache.spark.mllib.linalg.Vector] = {                                  
         a++b
     }
     val temp=alledges.join(F,alledges("node")===F("node")).toDF("fromnode","node","node1","vector").select("fromnode","node","vector")
     val tocompute=temp.join(F,temp("node")===F("node")).toDF("fromnode","tonode","fromvector","node","tovector").select("fromnode","tonode","fromvector","tovector").map(r=>((r.getLong(0),r.getAs[org.apache.spark.mllib.linalg.Vector](2)),Array(r.getAs[org.apache.spark.mllib.linalg.Vector](3)))).rdd.reduceByKey(aggregatorNeighVec).map(x=>(x._1._1,x._1._2,x._2))
     val result = tocompute.map{case(x,y,z)=>
       val fu = BDM.create(1,kvalue,y.toArray)
       
       val fusfT = (fu * BDM.create(kvalue, 1, sumF.data)).data(0)
       val fufuT = (fu * BDM.create(kvalue, 1, fu.data)).data(0)
       z.map {m =>
       val tmpdata=BDM.create(1,kvalue,m.toArray)
       var fvT = BDM.create(kvalue, 1, tmpdata.data);
       var fufvT = (fu * fvT).data(0);
       (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
     }.reduce(_+_)
     return result
}
 
def loglikelihood(F:DataFrame,sumF:breeze.linalg.DenseMatrix[Double],edgerdd:RDD[(Long,Long)],kvalue:Int,spark: SparkSession):Double ={
      import spark.implicits._
      //make (node,nodevector,Array[neighvector]) to make LLH compute easily
      val outedges=edgerdd.map(e => (e._1,e._2))
      val inedges=edgerdd.map(e => (e._2,e._1))
      val alledges=outedges.union(inedges).toDF("node","tonode")
      loglikelihood(F,sumF,alledges,kvalue,spark)
}



def Optimize(kvalue:Int,c2n:DataFrame,n2c:DataFrame,trainrdd:RDD[(Long,Long)],testrdd:RDD[(Long,Long)],S:Array[Long],uset:List[Long],spark: SparkSession):(DataFrame,Double)=
{
    import spark.implicits._
    val sc=spark.sparkContext 
	val sumfdata=c2n.map(x=>(x.getLong(0),x.getAs[Vector](1).toSparse.values.sum)).collect.sortBy(_._1).map(x=>x._2)
    
    
    var F=n2c
    var sumF = BDM.create(1,kvalue,sumfdata)
    
    val MIN_P_ = 0.0001
    val MAX_P_ = 0.9999
    val MIN_F_ = 0.0
    val MAX_F_ = 1000.0

     
     def step(Fu:BDM[Double], stepSize:Double, direction:BDM[Double]):BDM[Double]= {
       BDM.create(1,kvalue,(Fu + stepSize * direction).data.map{x =>
         math.min(math.max(x ,MIN_F_),MAX_F_)})
     }
    
     def aggregatorNeighandVec(a: (Array[Long],Array[org.apache.spark.mllib.linalg.Vector]), b: (Array[Long],Array[org.apache.spark.mllib.linalg.Vector])): (Array[Long],Array[org.apache.spark.mllib.linalg.Vector]) = {                                  
         (a._1++b._1,a._2++b._2)
     }

     def mergestep(a:(Double, breeze.linalg.DenseMatrix[Double],breeze.linalg.DenseMatrix[Double]),b:(Double, breeze.linalg.DenseMatrix[Double],breeze.linalg.DenseMatrix[Double])):(Double,breeze.linalg.DenseMatrix[Double],breeze.linalg.DenseMatrix[Double])=
     {
		if(a._1>b._1)
		{
			a
		}
		else
		{
			b
		}
     }

     var alpha = 0.05
     var beta = 0.1
     var MaxIter =15
     //var oldFuT = BDM.create(kvalue,1,fu.data)
     var stepSize: Double = 1.0
     var listSearch: List[Double] = List(1.0)
     for(i <- 1 to MaxIter) {
       stepSize *=beta
       listSearch = stepSize::listSearch
     }
     var liststepSizeRDD = sc.makeRDD(listSearch)
     def backtrackingLineSearchs(uset:List[Long],edgerdd:RDD[(Long,Long)]): Double =
     {
      	val outedges=edgerdd.map(e => (e._1,e._2))
      	val inedges=edgerdd.map(e => (e._2,e._1))
      	val alledges=outedges.union(inedges).toDF("node","tonode")
      	val temp=alledges.join(F,alledges("node")===F("node")).toDF("fromnode","node","node1","vector").select("fromnode","node","vector")
      	val tocompute=temp.join(F,temp("node")===F("node")).toDF("fromnode","tonode","fromvector","node","tovector").select("fromnode","tonode","fromvector","tovector").map(r=>((r.getLong(0),r.getAs[org.apache.spark.mllib.linalg.Vector](2)),(Array(r.getLong(1)),Array(r.getAs[org.apache.spark.mllib.linalg.Vector](3))))).rdd.reduceByKey(aggregatorNeighandVec).map(x=>(x._1._1,x._1._2,x._2._2,x._2._1))

        //PRE-BACKTRACKING LINE SEARCH
        var result1 = tocompute.filter(x=> uset.contains(x._1)).map{case(ux,y,z,neighs)=>
        var fu = BDM.create(1,kvalue,y.toArray);
        var fusfT = (fu*BDM.create(kvalue,1,sumF.data)).data(0);
        var sf = sumF
        var fufuT = (fu*BDM.create(kvalue,1,fu.data)).data(0);
        var kq =z.map {m =>
            var fv = BDM.create(1,kvalue,m.toArray);
            var fvT = BDM.create(kvalue, 1, fv.data);
            var fufvT = (fu * fvT).data(0);
            var fufvTrange = math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_);
            (math.log(1- fufvTrange)+ fufvT,fv*(1/(1 - fufvTrange)))}.reduce((a,b) => (a._1+b._1,a._2+b._2))
        (ux,fu, kq._2 - sf + fu,kq._1 - fusfT + fufuT,neighs,z)//(u,fu,grad,llh,neighbor,neighvec)
        }.persist()
    
        //TODO
        // BACKTRACKING LINE SEARCH
        var kq1 = result1.cartesian(liststepSizeRDD).coalesce(1000).map{ case (x,stepx) =>
        var newfu = step(x._2,stepx,x._3);
        val fuT = BDM.create(kvalue,1,newfu.data);
        val sfT = BDM.create[Double](kvalue, 1, sumF.data) - BDM.create(kvalue,1,x._2.data) + fuT;
        var result = x._6.map{m =>
            val tmpdata=BDM.create(1,kvalue,m.toArray);
            var xc = newfu*BDM.create(kvalue,1,tmpdata.data)
            math.log(1- math.min(math.max(math.exp(-xc.data(0)),MIN_P_),MAX_P_))+ xc.data(0)
        }.reduce(_+_) -(newfu*sfT).data(0)+(newfu*fuT).data(0)
        (x._1,stepx,result >= (x._4 + (alpha*stepx*x._3 * BDM.create(kvalue,1,x._3.data)).data.reduce(_+_)),x._2,x._3)
	}.filter(_._3).map(x => (x._1,(x._2,x._4,x._5))).reduceByKey(mergestep).map(x=>(x._1,x._2._2,step(x._2._2,x._2._1,x._2._3))).persist()
        //UPDATE F and SUMF
        var Sx: Array[Long] = Array()
        if(kq1.count() != 0)
        {
            Sx = kq1.map(_._1).collect
            //F = F.rdd.filter(x => !Sx.contains(x.getLong(0))).union(kq1.map(x=> (x._1,x._3)))
            F=F.filter(x => !Sx.contains(x.getLong(0))).union(kq1.map(x=> (x._1,new DenseVector(x._3.toArray).toSparse)).toDF("node","vector"))
            var changeFu =kq1.map(x=>(x._2,x._3)).reduce((a,b) => (a._1+ b._1,a._2+b._2))
            sumF = sumF - changeFu._1 + changeFu._2
        }
        val LLH = loglikelihood(F,sumF,alledges,kvalue,spark)
	    return LLH
  }




  def MBSGD():Unit={
      var LLHold = loglikelihood(F,sumF,trainrdd,kvalue,spark)
      println("LLH: " + LLHold)
      //var size = G.vertices.count()
      breakable {
        var i =0
        while (true) {
            //to train it on the trainrdd
            var newLLH = backtrackingLineSearchs(uset,trainrdd)
            i+= uset.size
            println(" Inter: "+ i + " LLH: " + newLLH )
            if (math.abs(1 - newLLH/ LLHold) < 0.0001) {break}
            LLHold = newLLH
      }
    }
  }

  MBSGD()
  //to verify the effect on the testrdd
  val verifyll=loglikelihood(F,sumF,testrdd,kvalue,spark)
  return (F,verifyll)
}




def run(graphpath:String):Unit={
    val spark = SparkSession.builder()
          .config("spark.sql.shuffle.partitions", 1000)
          .appName(s"BigClam").getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    @transient
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    //for simple data
    //alert!!!所有的节点id最好是紧凑的格式，例如id为0~max(num_node)-1的形式,这样能最大化减少运行中的内存占用
    val edgerdd=spark.read.textFile(graphpath).filter(!_.contains("#")).map(_.split("\t")).distinct.map(r=>(r(0).toLong,r(1).toLong)).rdd.persist()
    val edges=edgerdd.map(e => Edge(e._1,e._2, 1L))
    val defaultvalue=""
    val G=Graph.fromEdges(edges,defaultvalue)
  
    val epsCommForce = 1e-6
    
    val collectNeighbor = G.ops.collectNeighborIds(EdgeDirection.Either).map{case(id,attr)=>(id,attr)}.persist()
    val EgoNeighbor = collectNeighbor.map(x =>(x._1,Array(x._1)++x._2))

    def aggregatorForTag(a: (Array[Long], Array[Long]), b: (Array[Long], Array[Long])): (Array[Long], Array[Long]) = {                                    
         ((a._1++b._1).distinct,(a._2++b._2))
    }

    def aggregatorForMinimal(a: Double, b: Double): Double = {                                    
             if(a<=b)
	     {
		        a
	     } 
             else
	     {
                b
	     }
    }
    
    def NeighMinimal(a: (Double,Long), b: (Double,Long)): (Double,Long) = {                                    
             if(a._1<b._1)
	     {
		a
	     }else if(a._1==b._1)
	     {
		if(a._2<=b._2)
		{
		   a
		}
		else
		{
		   b
		}
	     }
	     else
	     {
                b
	     }
    }


    def conductanceLocalMin(): Array[Long]={
            // compute conductance for all nodes
            val egonet=EgoNeighbor
            val sigmaDegres = sc.broadcast((G.inDegrees ++ G.outDegrees).reduceByKey(_+_).map(_._2).reduce(_+_))           
            val outedges=edgerdd.map(e => (e._1,e._2))
            val inedges=edgerdd.map(e => (e._2,e._1))
            val selfedges=G.vertices.map{case(id,attr)=>id}.map(x=>(x,x))
            val egonetedges=outedges.union(inedges).union(selfedges).toDF("fromnode","node")

            val onedegreeneigh=collectNeighbor.toDF("node","neigharray")
            val twodegreeneigh=egonetedges.join(onedegreeneigh,egonetedges("node")===onedegreeneigh("node")).toDF("fromnode","tonode","tonode1","neigharray").select("fromnode","tonode","neigharray").map(r=>(r.getLong(0),(Array(r.getLong(1)),r.getAs[Seq[Long]](2).toArray))).rdd

            //to make (x,1neigh(x),2neigh(x))
            val oneandtwoneigh=twodegreeneigh.reduceByKey(aggregatorForTag) 
            val ConductanceRDD =oneandtwoneigh.map{case(x,(y,z))=>
              var cut_S = z.map(i => if(y.contains(i)) 0.0 else 1.0).filter(m => m == 1.0).size;
              var vol_S = z.size-cut_S;
              var vol_T = sigmaDegres.value-vol_S-cut_S*2;
              (x,if(vol_S == 0) 0 else if(vol_T == 0) 1 else cut_S.toDouble/math.min(vol_S,vol_T))
            }
         
            val ConductanceDF=ConductanceRDD.toDF("node","conducu")
	        val alledges=outedges.union(inedges).toDF("fromnode","node") 
            val minimal_1=alledges.join(ConductanceDF,alledges("node")===ConductanceDF("node")).toDF("node","tonode","node1","dstconducu").select("node","tonode","dstconducu")
	        val minimal_2=minimal_1.join(ConductanceDF,minimal_1("node")===ConductanceDF("node")).toDF("fromnode","tonode","dstconducu","node1","srcconducu").select("fromnode","tonode","dstconducu","srcconducu")
	        //TODO,i am not sure which method is correct
	        //method 1
	        val indx=minimal_2.map(r=>((r.getLong(0),r.getDouble(3)),r.getDouble(2))).rdd.reduceByKey(aggregatorForMinimal).filter{case ((x,y),z)=>y<=z}.map{case ((x,y),z)=>x}.collect.distinct

	        //method 2
	        //val indx=minimal_2.map(r=>(r.getLong(0),(r.getDouble(2),r.getLong(1)))).rdd.reduceByKey(NeighMinimal).map(_._2._2).collect.distinct
            sigmaDegres.destroy()
            return indx
    }

    
    var uset = G.vertices.map(_._1).collect.toList
    var S = conductanceLocalMin()
    
    //kvalue need to be less than S.size
    var kvalue=100
    val vcount=uset.size
    println("kvalue: " + kvalue)
    println("S.size: " + S.size)
    println("uset: " + vcount)
    
    def randomIndexedRow(index:Long,n : Int ):IndexedRow={
        IndexedRow(index.toLong,Vectors.dense(Array.fill(n)(Random.nextInt(2).toDouble)).toSparse)
    }


    //to init the cluster-affilation matrix,c2n uses cluster as row and node as column,and n2c use nodes as row,cluster as column
    def initNeighborComF(): (DataFrame,DataFrame)={
            // Get S set which is conductance of locally minimal
            val sinx=sc.broadcast(S.zipWithIndex.toMap)

            //we need to make sure nodeid<=vcount-1
            var rowsselect = collectNeighbor.filter(x=>S.contains(x._1)).map(x =>{
                val ind=(x._2++Array(x._1)).distinct
                val value=Array.fill(ind.size)(1.0);
                val y=new SparseVector(vcount, ind.map(_.toInt).sorted, value);
                (x._1,y)
            }).toDF("node","vector")
            var rows=rowsselect.map(r=>IndexedRow(sinx.value(r.getLong(0)),r.getAs[Vector](1))).rdd

            
            val columnselect=collectNeighbor.map(x=>{
                    val size=x._2.filter(r=>S.contains(r)).distinct.size;
                    val ind=x._2.filter(r=>S.contains(r)).distinct.map(r=>sinx.value(r)).sorted;
                    val value=Array.fill(size)(1.0);
                    val y=new SparseVector(S.size, ind, value);
                    (x._1,y)}).toDF("node","vector")

            sinx.destroy()
            (rowsselect,columnselect)
     }
     val (c2n:DataFrame,n2c:DataFrame)=initNeighborComF
     collectNeighbor.unpersist()
    
     val Array(trainrdd,testrdd) = edgerdd.randomSplit(Array(0.8, 0.2))
     //change the kvalue to see the variants of value,select the kvalue which makes value biggest
     var (finalmatrix:DataFrame,value:Double)=Optimize(kvalue,c2n,n2c,trainrdd,testrdd,S,uset,spark)
     edgerdd.unpersist()

    //print the best result 
     val F=finalmatrix
     var e = 2.0*G.collectEdges(EdgeDirection.Either).count/(G.vertices.count*(G.vertices.count - 1))
     e = math.sqrt(-math.log(1-e))
     var Com = F.map(r=>(r.getLong(0),r.getAs[Vector](1).toArray)).map{
         x => var Fmax = x._2.max;
             (
             x._1, if(Fmax < e){
             Vectors.dense(x._2.map(value =>if (value == Fmax) 1.0 else 0.0)).toSparse.indices
              }else
              {
             Vectors.dense(x._2.map(value =>if (value>=e) 1.0 else 0.0)).toSparse.indices
             })
     }
     Com.flatMap{case(x,y) => y.map(c => (c,x))}.rdd.groupByKey().repartition(1000).saveAsTextFile("/tmp/bigclam/kq-youhua.txt")

  }

}

object BigClam{
    def main(args: Array[String]) {
        print(args.size)
        print(args)
        val Array(graphpath) = args
        new BigClam().run(graphpath)
    }
    
}




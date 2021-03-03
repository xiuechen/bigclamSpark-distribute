package com.baidu.yundata.kg.algorithm.bigclam



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
import scala.collection.{immutable, mutable}





class BigClamPregel extends org.apache.spark.internal.Logging with Serializable {
def MIN_P_ = 0.0001
def MAX_P_ = 0.9999
def MIN_F_ = 0.0
def MAX_F_ = 1000.0 

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
      //构造(node,nodevector,Array[neighvector])
      val outedges=edgerdd.map(e => (e._1,e._2))
      val inedges=edgerdd.map(e => (e._2,e._1))
      val alledges=outedges.union(inedges).toDF("node","tonode")
      loglikelihood(F,sumF,alledges,kvalue,spark)
}

def Optimize(kvalue:Int,n2c:DataFrame,trainrdd:RDD[(Long,Long)],testrdd:RDD[(Long,Long)],S:Array[Long],uset:List[Long],spark: SparkSession):(DataFrame,Double)=
{
    import spark.implicits._
    val sc=spark.sparkContext 
   
 
    var F=n2c.rdd.map(r=>(r.getLong(0),r.getAs[Vector](1)))
    
    val edges=trainrdd.map(e => Edge(e._1,e._2, 1L))
    val defaultvalue=""
    val G=Graph.fromEdges(edges,defaultvalue)
    
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
    def step(Fu:BDM[Double], stepSize:Double, direction:BDM[Double]):BDM[Double]= {
         BDM.create(1,kvalue,(Fu + stepSize * direction).data.map{x =>
         math.min(math.max(x ,MIN_F_),MAX_F_)})
    }

    val initG=G.outerJoinVertices(F){
        (vid,vdata,vector)=>{
        //val defaultvec=Vectors.dense(Array.fill(S.size)(Random.nextInt(2).toDouble)).toSparse
	    val defaultvec=Vectors.dense(Array.fill(S.size)(0.0)).toSparse
        (vector.getOrElse(defaultvec),Array[Long](),mutable.Map[Long,Vector](),0.toDouble,true)
    }
    }

    var sumF=initG.vertices.map{case (id,attr)=>
		    val fu=BDM.create(1,kvalue,attr._1.toArray)
            fu
    }.reduce(_+_)


    def vertexProgram(id: VertexId, attr: (Vector, Array[Long],mutable.Map[Long,Vector],Double,Boolean), msgSum: Array[(Long,Vector)]): (Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean) = {
        if(msgSum.isEmpty)
        {
            return attr;
        }
        else
        {
            var neigharr=attr._2
            if(attr._2.isEmpty && !msgSum.isEmpty)
            {
               neigharr=msgSum.map(x=>x._1)              
            }
            
            //更新neigh
		    for((key,value)<-msgSum.toMap)
		    {
		       attr._3.update(key,value)
		    }
            //计算llh以及grad
		    val fu=BDM.create(1,kvalue,attr._1.toArray)
		    val fusfT = (fu * BDM.create(kvalue, 1, sumF.data)).data(0)
            val fufuT = (fu * BDM.create(kvalue, 1, fu.data)).data(0)
		    val sf=sumF
		    val kq=neigharr.map{m=>
		    	val fv=BDM.create(1,kvalue,attr._3.get(m).get.toArray);
		    	var fvT = BDM.create(kvalue, 1,fv.data);
		    	var fufvT = (fu * fvT).data(0);
		    	var fufvTrange = math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_);
		    	((math.log(1 - fufvTrange)+ fufvT),fv*(1/(1 - fufvTrange)))}.reduce((a,b) => (a._1+b._1,a._2+b._2))
            val llh=kq._1- fusfT + fufuT
            val grad=kq._2- sf + fu
            if(attr._4!=0 && math.abs(1 - llh/ attr._4) < 0.0001)
            {
                return (attr._1,neigharr,attr._3,llh,false)
            }
            //计算新的newvec
		    val stepselected=listSearch.map(stepx=>{
		    	val newfu=step(fu,stepx,grad);
		    	val newfuT = BDM.create(kvalue,1,newfu.data);
		    	val newsfT = BDM.create[Double](kvalue, 1, sumF.data) - BDM.create(kvalue,1,fu.data) + newfuT;			
		    	var newllh = attr._2.map{m =>
		    		val tmpdata=BDM.create(1,kvalue,attr._3.get(m).get.toArray);
		    		var xc = newfu*BDM.create(kvalue,1,tmpdata.data);
		    		math.log(1- math.min(math.max(math.exp(-xc.data(0)),MIN_P_),MAX_P_))+ xc.data(0);
		    	}.reduce(_+_) -(newfu*newsfT).data(0)+(newfu*newfuT).data(0);
		    	(stepx,(newllh,newfu))	}
		    ).filter{case(a:Double,(b:Double,c:BDM[Double]))=>b>=(llh + (alpha*a*grad * BDM.create(kvalue,1,grad.data)).data.reduce(_+_))}.sortBy(_._1)
		    if(stepselected.isEmpty)
		    {
		    	(attr._1,neigharr,attr._3,llh,false)
		    }
		    else	
		    {
		    	val selected=stepselected.last
                val newfu=selected._2._2
                sumF=sumF+newfu-fu
                val newvec=Vectors.dense(newfu.toArray).toSparse
                (newvec,neigharr,attr._3,llh,true)
		    }
               
        }
    }
   
    /*def sendMessage(edge: EdgeTriplet[(Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), Long]) = {    
        if (edge.dstAttr._4 == true) {
            Iterator((edge.srcId, Array((edge.dstId,edge.dstAttr._1))))
        }
        else
        {
            Iterator.empty
        }
        if (edge.srcAttr._4 == true) {
            Iterator((edge.dstId, Array((edge.srcId,edge.srcAttr._1))))
        }
        else
        {
            Iterator.empty
        }
    }*/
    
    def sendMessage(edge: org.apache.spark.graphx.EdgeContext[(Vector, Array[Long],mutable.Map[Long,Vector],Double,Boolean), Long,Array[(Long,Vector)]]) = { 
        if (edge.dstAttr._5 == true) {
            edge.sendToSrc(Array((edge.dstId,edge.dstAttr._1)))
        }
        if (edge.srcAttr._5 == true) {
            edge.sendToDst(Array((edge.srcId,edge.srcAttr._1)))
        }
    }
    
    def messageCombiner(a: Array[(Long,Vector)], b: Array[(Long,Vector)]): Array[(Long,Vector)] = a ++ b
    val initialMessage = Array[(Long,Vector)]()
    val vp={
            (id: VertexId, attr: (Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), msgSum: Array[(Long,Vector)]) =>vertexProgram(id, attr, msgSum)
    } 
    
    var g = initG.mapVertices((vid, vdata) => vp(vid, vdata, initialMessage)).cache()
    //var messages = g.mapReduceTriplets(sendMessage, messageCombiner)
    var messages = g.aggregateMessages[Array[(Long,Vector)]](sendMessage, messageCombiner)
    var activeMessages = messages.count()
    var prevG: Graph[(Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), Long] = null
    var LLHold=g.vertices.values.map(_._4).reduce(_+_)
     println("LLH: " + LLHold) 
    var newLLH=LLHold

    var i = 0
    var flag = true
    while (activeMessages > 0 && i < Int.MaxValue && flag) {
        prevG = g
        g = g.joinVertices(messages)(vp).cache()
        val oldMessages = messages
        val oldsumF=sumF
        //messages = g.mapReduceTriplets(sendMessage, messageCombiner,Some((oldMessages,EdgeDirection.Either))).cache()
        messages = g.aggregateMessages[Array[(Long,Vector)]](sendMessage, messageCombiner).cache()
        activeMessages = messages.count()
		newLLH=g.vertices.values.map(_._4).reduce(_+_)
        println("i: " +i+" llh:"+newLLH+" msgcount:"+ activeMessages)
        logInfo("Pregel finished iteration " + i+" llh:"+newLLH)
        oldMessages.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
		if ( LLHold != 0.0 && math.abs(1 - newLLH/ LLHold) < 0.0001) {flag = false}
		LLHold=newLLH
        sumF=g.vertices.map{case (id,attr)=>
	    	    val fu=BDM.create(1,kvalue,attr._1.toArray)
                fu
        }.reduce(_+_)
        i=i+1
    }
    messages.unpersist(blocking = false)
    val finalgraph=g

    //val finalgraph=Pregel(initG, initialMessage, activeDirection = EdgeDirection.Either)(vp, sendMessage, messageCombiner)

     
    val Fdf=finalgraph.vertices.map{case (id,attr)=>(id,attr._1)}.toDF("node","vector")
    val verifyll=loglikelihood(Fdf,sumF,testrdd,kvalue,spark)
    return (Fdf,verifyll)
}



def run(graphpath:String):Unit={
    val spark = SparkSession.builder()
          .config("spark.sql.shuffle.partitions", 1000)
          .appName(s"BigClam").getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val edgerdd=spark.read.textFile(graphpath).filter(!_.contains("#")).map(_.split("\t")).distinct.map(r=>(r(0).toLong,r(1).toLong)).rdd.persist()

    val edges=edgerdd.map(e => Edge(e._1,e._2, 1L))
    val defaultvalue=""
    val G=Graph.fromEdges(edges,defaultvalue)
  
    
    
    val epsCommForce = 1e-6
    
    //构造Neightborbc,key为节点,value为节点对应的邻居
    val collectNeighbor = G.ops.collectNeighborIds(EdgeDirection.Either).map{case(id,attr)=>(id,attr)}.persist()


    val Neightborhoodbc = collectNeighbor.map(x =>(x._1,Array(x._1)++x._2))

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
            val egonet=Neightborhoodbc
            val sigmaDegres = sc.broadcast((G.inDegrees ++ G.outDegrees).reduceByKey(_+_).map(_._2).reduce(_+_))           
            //将egonet中的(x,neigh(x))展开为(x,n1),(x,n2),(x,n2)
            //等价于将edges反过来和原来的union，并且加上连接上自身的边
            val outedges=edgerdd.map(e => (e._1,e._2))
            val inedges=edgerdd.map(e => (e._2,e._1))
            val selfedges=G.vertices.map{case(id,attr)=>id}.map(x=>(x,x))
            val egonetedges=outedges.union(inedges).union(selfedges).toDF("fromnode","node")

            //将egonetedges和collectNeighbor进行join，找到二阶邻居
            val onedegreeneigh=collectNeighbor.toDF("node","neigharray")
            
            val twodegreeneigh=egonetedges.join(onedegreeneigh,egonetedges("node")===onedegreeneigh("node")).toDF("fromnode","tonode","tonode1","neigharray").select("fromnode","tonode","neigharray").map(r=>(r.getLong(0),(Array(r.getLong(1)),r.getAs[Seq[Long]](2).toArray))).rdd

             //通过reducebykey聚合(x,1neigh(x),2neigh(x))
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
	        //TODO，以下两种算法不清楚哪种对
	        //method 1
	        val indx=minimal_2.map(r=>((r.getLong(0),r.getDouble(3)),r.getDouble(2))).rdd.reduceByKey(aggregatorForMinimal).filter{case ((x,y),z)=>y<=z}.map{case ((x,y),z)=>x}.collect.distinct

	        //method 2
	        //val indx=minimal_2.map(r=>(r.getLong(0),(r.getDouble(2),r.getLong(1)))).rdd.reduceByKey(NeighMinimal).map(_._2._2).collect.distinct
       
            sigmaDegres.destroy()
            return indx
    }

    
    val Array(trainrdd,testrdd) = edgerdd.randomSplit(Array(0.8, 0.2))
    var uset = G.vertices.map(_._1).collect.toList
   
    var S=conductanceLocalMin()
        
    //var kvalue=S.size
    var kvalue=100
    val vcount=uset.size
    println("kvalue: " + kvalue)
    println("S.size: " + S.size)
    println("uset: " + vcount)
    

    //初始化node和cluster之间的二部图矩阵F
    def initNeighborComF(): DataFrame={
            // Get S set which is conductance of locally minimal
            val sinx=sc.broadcast(S.zipWithIndex.toMap)
            //求解以node id为index，node id和各个cluster连接的权重矩阵
            val columnselect=Neightborhoodbc.map(x=>{
                    var ind=x._2.filter(r=>S.contains(r)).distinct.map(r=>sinx.value(r)).sorted;
                    var addColumn:SparseVector = null
                    if(S.size < kvalue)
                    {
                        val diffsize=kvalue-S.size
                        addColumn=Vectors.dense(Array.fill(diffsize)(Random.nextInt(2).toDouble)).toSparse
                    }else
                    {
                        ind=ind.filter(x=>x<=kvalue-1)
                    }
                    if(addColumn!=null)
                    {
                        val addinx=addColumn.indices.map(x=>x+S.size)
                        ind=ind ++ addinx
                    }
                    var value=Array.fill(ind.size)(1.0);
                    val y=new SparseVector(kvalue, ind, value);
                    (x._1,y)}).toDF("node","vector")

            sinx.destroy()
            columnselect 
     }
     val n2c=initNeighborComF
     collectNeighbor.unpersist()
     var (finalmatrix:DataFrame,value:Double)=Optimize(kvalue,n2c,trainrdd,testrdd,S,uset,spark)

     edgerdd.unpersist()
    
    //比较不同kvalue下的value大小，取最大的value对应的k，以及F作为分类的结果,这里省略
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
     def aggfornodes(a: Array[Long], b: Array[Long]): Array[Long] = {                                    
         a++b
     }
     Com.flatMap{case(x,y) => y.map(c => (c,Array(x)))}.rdd.reduceByKey(aggfornodes).coalesce(100).map(x=>(x._1,x._2.mkString(","))).saveAsTextFile("/tmp/kq-youhua.txt")

  }

}

object BigClamPregel{
    def main(args: Array[String]) {
        print(args.size)
        print(args)
        val Array(graphpath) = args
        new BigClamPregel().run(graphpath)
    }
    
}




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
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import com.baidu.yundata.kg.util.checkpoint._



class BigClamPregel extends org.apache.spark.internal.Logging with Serializable {
def MIN_P_ = 0.0001
def MAX_P_ = 0.9999
def MIN_F_ = 0.0
def MAX_F_ = 1000.0

def mydot(x: SparseVector, y: SparseVector):Double={
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
}

def mydot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
}

def mydot(x: DenseVector, y: DenseVector): Double = {
    val xValues = x.values
    val yValues = y.values
    val nnz = x.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(k)
      k += 1
    }
    sum
}





 
def maptosparse(a:scala.collection.immutable.Map[Int,Double],kvalue:Int):SparseVector={
  val info=a.toArray.sortBy(_._1)
 val ind=info.map(x=>x._1) 
 val value=info.map(x=>x._2) 
 val y=new SparseVector(kvalue, ind, value);
 y
}

def sparsetomap(a:SparseVector):scala.collection.immutable.Map[Int,Double]={
 val y=a.indices.zip(a.values).toMap;
 y
}


def sum(a:scala.collection.immutable.Map[Int,Double],b:DenseVector):DenseVector={
    val yValues=b.values
    val kvalue=b.size
    val newvalues=new Array[Double](kvalue)
       
    var k = 0
    while (k < kvalue) {
      var value1=a.get(k).getOrElse(0.0) + yValues(k)
      newvalues(k)=value1
      k += 1
    }
    new DenseVector(newvalues)
  
}

def sum(a:scala.collection.immutable.Map[Int,Double],b:DenseVector,result:DenseVector):Unit={
    val yValues=b.values
    val kvalue=b.size
       
    var k = 0
    while (k < kvalue) {
      var value1=a.get(k).getOrElse(0.0) + yValues(k)
      result.values(k)=value1
      k += 1
    }
}

def sum(a:scala.collection.immutable.Map[Int,Double],b:scala.collection.immutable.Map[Int,Double]):scala.collection.immutable.Map[Int,Double]=
{
      val sumHashMap: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]
      sumHashMap ++= a
      for ((index:Int, value:Double) <- b) {
        if (sumHashMap.contains(index)) {
          val value1=sumHashMap.get(index).getOrElse(0.0) + value
          if(value1==0.0)
          {
            sumHashMap.remove(index)
          }
          else
          {
            sumHashMap.update(index, sumHashMap.get(index).getOrElse(0.0) + value)
          }
        }
        else {
          if(value!=0.0)
          {
            sumHashMap.put(index, value)
          }
        }
      }
      collection.immutable.HashMap(sumHashMap.toSeq: _*)
}

def stepsum(a:scala.collection.immutable.Map[Int,Double],b:scala.collection.immutable.Map[Int,Double]):scala.collection.immutable.Map[Int,Double]=
{
      val sumHashMap: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]
      sumHashMap ++= a
      for ((index:Int, value:Double) <- b) {
        if (sumHashMap.contains(index)) {
           var value1=sumHashMap.get(index).getOrElse(0.0) + value
           value1=math.min(math.max(value1,MIN_F_),MAX_F_)
           if(value1==0.0)
           {
             sumHashMap.remove(index)
           }
           else
           {
              sumHashMap.update(index,value1)
           }
        }
        else {
          val value1=math.min(math.max(value,MIN_F_),MAX_F_)
          if(value1!=0.0)
          {
            sumHashMap.put(index, value)
          }
        }
      }
      collection.immutable.HashMap(sumHashMap.toSeq: _*)
}

def myreduce(a:scala.collection.immutable.Map[Int,Double],b:scala.collection.immutable.Map[Int,Double]):scala.collection.immutable.Map[Int,Double]=
{
      val sumHashMap: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]
      sumHashMap ++= a
      for ((index:Int, value:Double) <- b) {
        if (sumHashMap.contains(index)) {
          sumHashMap.update(index, sumHashMap.get(index).getOrElse(0.0) - value)
        }
        else {
          sumHashMap.put(index, -value)
        }
      }
      collection.immutable.HashMap(sumHashMap.toSeq: _*)
    
}

def myreduce(a:scala.collection.immutable.Map[Int,Double],b:DenseVector):DenseVector=
{
    val yValues=b.values
    val kvalue=b.size
    val newvalues=new Array[Double](kvalue)
       
    var k = 0
    while (k < kvalue) {
      var value1=a.get(k).getOrElse(0.0) - yValues(k)
      newvalues(k)=value1
      k += 1
    }
    new DenseVector(newvalues)
    
}

def loglikelihood(F:DataFrame,sumF:scala.collection.immutable.Map[Int,Double],alledges:DataFrame,kvalue:Int,spark: SparkSession):Double ={
    import spark.implicits._
    val MIN_P_ = 0.0001
    val MAX_P_ = 0.9999
     def aggregatorNeighVec(a: Array[org.apache.spark.mllib.linalg.Vector], b: Array[org.apache.spark.mllib.linalg.Vector]): Array[org.apache.spark.mllib.linalg.Vector] = {                                  
         a++b
     }
     val temp=alledges.join(F,alledges("node")===F("node")).toDF("fromnode","node","node1","vector").select("fromnode","node","vector")
     val tocompute=temp.join(F,temp("node")===F("node")).toDF("fromnode","tonode","fromvector","node","tovector").select("fromnode","tonode","fromvector","tovector").map(r=>((r.getLong(0),r.getAs[org.apache.spark.mllib.linalg.Vector](2)),Array(r.getAs[org.apache.spark.mllib.linalg.Vector](3)))).rdd.reduceByKey(aggregatorNeighVec).map(x=>(x._1._1,x._1._2,x._2))
     val result = tocompute.map{case(x,y,z)=>
       val fu=y.toSparse
       val sf=maptosparse(sumF,kvalue).toDense
       val fusfT=mydot(fu,sf)
       val fufuT=mydot(fu,fu)
       z.map {m =>
       var fvT = m.toSparse;
       var fufvT = mydot(fu,fvT);
       (math.log(1 - math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_))+ fufvT)}.reduce(_+_) - fusfT + fufuT
     }.reduce(_+_)
     return result
}
 
def loglikelihood(F:DataFrame,sumF:scala.collection.immutable.Map[Int,Double],edgerdd:RDD[(Long,Long)],kvalue:Int,spark: SparkSession):Double ={
      import spark.implicits._
      //构造(node,nodevector,Array[neighvector])
      val outedges=edgerdd.map(e => (e._1,e._2))
      val inedges=edgerdd.map(e => (e._2,e._1))
      val alledges=outedges.union(inedges).toDF("node","tonode")
      loglikelihood(F,sumF,alledges,kvalue,spark)
}

def multi(stepsize:Double,direction:scala.collection.immutable.Map[Int,Double],kvalue:Int):scala.collection.immutable.Map[Int,Double]={
  val info=direction.toArray.sortBy(_._1)
 val ind=info.map(x=>x._1) 
 val value=info.map(x=>x._2*stepsize) 
 val y=new SparseVector(kvalue, ind, value);
 sparsetomap(y)
}


def step(Fu:SparseVector, stepSize:Double, direction:scala.collection.immutable.Map[Int,Double],kvalue:Int):SparseVector= {
val y=stepsum(sparsetomap(Fu),multi(stepSize,direction,kvalue))
val info=y.toArray.sortBy(_._1)
val ind=info.map(x=>x._1) 
val value=info.map(x=>x._2)
val y1=new SparseVector(kvalue, ind, value);
y1	 
}


def step(Fu:SparseVector, stepSize:Double, direction:DenseVector):SparseVector= {
    val fumap=sparsetomap(Fu)
    val yValues=direction.values
    val kvalue=direction.size
    val newvalues=new Array[Double](kvalue)
       
    var k = 0
    while (k < kvalue) {
      var value1=fumap.get(k).getOrElse(0.0) +yValues(k)*stepSize
      value1=math.min(math.max(value1,MIN_F_),MAX_F_)
      newvalues(k)=value1
      k += 1
    }
    new DenseVector(newvalues).toSparse
}

def sumllhgrad(a:(Double,scala.collection.immutable.Map[Int,Double]),b:(Double,scala.collection.immutable.Map[Int,Double])):(Double,scala.collection.immutable.Map[Int,Double])=
    {
          val sumHashMap: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]
          sumHashMap ++= a._2
          for ((index:Int, value:Double) <- b._2) {
            if (sumHashMap.contains(index)) {
              sumHashMap.update(index, sumHashMap.get(index).getOrElse(0.0) + value)
            }
            else {
              sumHashMap.put(index, value)
            }
          }
          (a._1+b._1,collection.immutable.HashMap(sumHashMap.toSeq: _*))
}

def Optimize(kvalue:Int,n2c:DataFrame,trainrdd:RDD[(Long,Long)],testrdd:RDD[(Long,Long)],spark: SparkSession):(DataFrame,Double)=
{
    import spark.implicits._
    val sc=spark.sparkContext 
   
 
    var F=n2c.rdd.map(r=>(r.getLong(0),r.getAs[Vector](1)))
    
    val checkpointInterval = sc.getConf.getInt("spark.graphx.pregel.checkpointInterval", -1)
    
    val edges=trainrdd.map(e => Edge(e._1,e._2, 1))
    val defaultvalue=""
    val G=Graph.fromEdges(edges,defaultvalue,StorageLevel.MEMORY_ONLY_SER,StorageLevel.MEMORY_ONLY_SER)


    val initG=G.outerJoinVertices(F){
        (vid,vdata,vector)=>{
        //val defaultvec=Vectors.dense(Array.S.size)(Random.nextInt(2).toDouble)).toSparse
	    val defaultvec=Vectors.dense(Array.fill(kvalue)(0.0)).toSparse
        (vector.getOrElse(defaultvec),Array[Long](),mutable.Map[Long,Vector](),0.toDouble,true)
    }
    }
    
    val graphCheckpointer = new MyPeriodicGraphCheckpointer[(Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), Int](checkpointInterval,sc)
    graphCheckpointer.update(initG)



    var sumF=initG.vertices.map{case (id,attr)=>
            val fumap=attr._1.toSparse.indices.zip(attr._1.toSparse.values).toMap
            fumap
    }.reduce(sum)
	var sf=sc.broadcast(maptosparse(sumF,kvalue).toDense)
    G.unpersist()



    def vertexProgram(id: VertexId, attr: (Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), msgSum: Array[(Long,Vector)]): (Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean) = {
        if(msgSum.isEmpty)
        {
            println("msgSum.isEmpty node: " +id+" return false")
            return attr;
        }
        else
        {
            val beg = System.currentTimeMillis()
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
            val fu=attr._1.toSparse
                val fusfT = mydot(fu,sf.value)
                var cost = System.currentTimeMillis() - beg
                println("nodeid: "+id +" llh cost1: " +cost)
                val fufuT = mydot(fu,fu)
                val kq=neigharr.map{m=>
                    val fv=attr._3.get(m).get.toSparse
                        var fufvT = mydot(fu,fv);
                    var fufvTrange = math.min(math.max(math.exp(-fufvT),MIN_P_),MAX_P_);
                    val fvmap=fv.indices.zip(fv.toSparse.values.map(x=>x*(1/(1 - fufvTrange)))).toMap
                        (((math.log(1 - fufvTrange)+ fufvT),fvmap))}.reduce(sumllhgrad(_,_))
                        val llh=kq._1- fusfT + fufuT
            val gradvec=myreduce(sum(kq._2,sparsetomap(fu)),sf.value)
            cost = System.currentTimeMillis() - beg
            println("nodeid: "+id +" llh cost2: " +cost+" llh:"+llh)
            if(attr._4!=0 && math.abs(1 - llh/ attr._4) < 0.0001)
            {
                println("nodeid: "+id +" llh:"+llh+" attr._4"+attr._4)
	    	    return (attr._1,neigharr,attr._3,llh,false)
            }
        
            var selected=(0.0,(0.0,fu)) 
            var flag=true
            var stepx = 1.0
            var alpha = 0.05
            var beta = 0.1
            var MaxIter =15
            var i=0


            val graddot=mydot(gradvec,gradvec) 
            cost = System.currentTimeMillis() - beg
            println("nodeid: "+id +" llh cost: " +cost+" llh:"+llh+" graddot:"+graddot)
            val newvalues=new Array[Double](kvalue)
            var newsfT=new DenseVector(newvalues)

            while(flag && i<MaxIter)
            {
	    	    //val newfu=step(fu,stepx,grad);
	    	    val newfu=step(fu,stepx,gradvec);
                cost = System.currentTimeMillis() - beg
                println("inside "+i+" nodeid: "+id +" llh cost: " +cost+" llh:"+llh)
		        sum(myreduce(sparsetomap(newfu),sparsetomap(fu)),sf.value,newsfT)
                cost = System.currentTimeMillis() - beg
                println("inside "+i+" nodeid: "+id +" llh cost: " +cost+" llh:"+llh)
	    	    var newllh = neigharr.map{m =>
			       val tmpdata=attr._3.get(m).get.toSparse
	    		   var xc = mydot(newfu,tmpdata);
	    		   math.log(1- math.min(math.max(math.exp(-xc),MIN_P_),MAX_P_))+ xc;
	    	       }.reduce(_+_) -(mydot(newfu,newsfT))+(mydot(newfu,newfu));
                
                cost = System.currentTimeMillis() - beg
                println("inside "+i+" nodeid: "+id +" llh cost: " +cost+" llh:"+llh+" newllh:"+newllh + "neigharr size:"+ neigharr.size)
               
                //if(newllh==llh || newllh== Double.NegativeInfinity ||(newllh != 0.0 && math.abs(1 - llh/ newllh) < 0.00001))
                if(newllh==llh || newllh== Double.NegativeInfinity)
                {
                    flag=false
                }
                //不符合要求，继续搜索
                else if(newllh<(llh + (alpha*stepx*graddot))) 
                {
                    i=i+1
                    flag=true  
                    stepx = stepx * beta
                }
                else
                {
                    selected=(stepx,(newllh,newfu))
                    flag=false
                }
            }
	       if(selected._1==0)
	       {
                cost = System.currentTimeMillis() - beg
         	    println("node: " +id+" return false"+" cost:"+cost + "step: "+stepx)
	    	    return (attr._1,neigharr,attr._3,llh,false)
	        }
	       else	
	       {
                cost = System.currentTimeMillis() - beg
        	    println("node: " +id+" return true"+" cost:"+cost+" step:"+selected._1+" stepx:"+stepx)
        	    val newfu=selected._2._2
        	    return (newfu,neigharr,attr._3,llh,true)
	        }
               
        }
    }
    def sendMessage(edge: org.apache.spark.graphx.EdgeContext[(Vector, Array[Long],mutable.Map[Long,Vector],Double,Boolean),Int,Array[(Long,Vector)]]) = { 
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


    var g = initG.mapVertices((vid, vdata) => vp(vid, vdata, initialMessage))
    
    graphCheckpointer.update(g)
    
    //var messages = g.mapReduceTriplets(sendMessage, messageCombiner)
    var messages = g.aggregateMessages[Array[(Long,Vector)]](sendMessage, messageCombiner)
    val messageCheckpointer = new MyPeriodicRDDCheckpointer[(VertexId, Array[(Long,Vector)])](
      checkpointInterval, sc,StorageLevel.MEMORY_ONLY_SER)
    messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, Array[(Long,Vector)])]])
    var activeMessages = messages.count()
    var prevG: Graph[(Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), Int] = null
    var LLHold=g.vertices.values.map(_._4).reduce(_+_)
     println("LLH: " + LLHold) 
    var newLLH=LLHold

    var i = 0
    var flag=true
    while (activeMessages > 0 && i < Int.MaxValue && flag) {
        prevG = g
        g = g.joinVertices(messages)(vp)
        graphCheckpointer.update(g)
        val oldMessages = messages
        val oldsumF=sumF
        messages = g.aggregateMessages[Array[(Long,Vector)]](sendMessage, messageCombiner)
        messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, Array[(Long,Vector)])]])
        activeMessages = messages.count()
		newLLH=g.vertices.values.map(_._4).reduce(_+_)
        println("i: " +i+" llh:"+newLLH+" msgcount:"+ activeMessages)
        logInfo("Pregel finished iteration " + i+" llh:"+newLLH)
        oldMessages.unpersist()
        prevG.unpersistVertices()
        prevG.edges.unpersist()
		if ( LLHold != 0.0 && math.abs(1 - newLLH/ LLHold) < 0.0001) {flag = false}
		LLHold=newLLH
        sumF=g.vertices.map{case (id,attr)=>
                val fumap=attr._1.toSparse.indices.zip(attr._1.toSparse.values).toMap
                fumap
        }.reduce(sum)
        sf.unpersist()
	    sf=sc.broadcast(maptosparse(sumF,kvalue).toDense)
        i=i+1
    }
    messageCheckpointer.unpersistDataSet()
    graphCheckpointer.deleteAllCheckpoints()
    messageCheckpointer.deleteAllCheckpoints()
    val finalgraph=g

    //val finalgraph=Pregel(initG, initialMessage, activeDirection = EdgeDirection.Either)(vp, sendMessage, messageCombiner)

     
    val Fdf=finalgraph.vertices.map{case (id,attr)=>(id,attr._1)}.toDF("node","vector")
    val verifyll=loglikelihood(Fdf,sumF,testrdd,kvalue,spark)
    println("verifyll: " + verifyll) 
    return (Fdf,verifyll)
}



def run(graphpath:String):Unit={
    val conf = new SparkConf
    conf.registerKryoClasses(Array(
                                classOf[BigClamPregel],
                                classOf[MyPeriodicGraphCheckpointer[(Vector,Array[Long],mutable.Map[Long,Vector],Double,Boolean), Int]],
                                classOf[MyPeriodicRDDCheckpointer[(VertexId, Array[(Long,Vector)])]],
                                classOf[mutable.Map[Long,Vector]],
                                classOf[Array[Long]],
                                classOf[Edge[Object]],
                                classOf[(VertexId,Object)],
                                classOf[RDD[Edge[Int]]]
                                ))
    val spark = SparkSession.builder()
          .config(conf)
          .appName(s"BigClamPregel").getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    @transient
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val edgerdd=spark.read.textFile(graphpath).filter(!_.contains("#")).map(_.split("\t")).distinct.map(r=>(r(0).toLong,r(1).toLong)).rdd.persist(StorageLevel.MEMORY_ONLY_SER)

    val edges=edgerdd.map(e => Edge(e._1,e._2, 1L))
    val defaultvalue=""
    val G=Graph.fromEdges(edges,defaultvalue,StorageLevel.MEMORY_ONLY_SER,StorageLevel.MEMORY_ONLY_SER)
  

    
    
    //var F:RDD[(Long,BDM[Double])] = _
    
    val epsCommForce = 1e-6
    
    //构造Neightborbc,key为节点,value为节点对应的邻居

    val collectNeighbor = G.ops.collectNeighborIds(EdgeDirection.Either).map{case(id,attr)=>(id,attr)}.persist(StorageLevel.MEMORY_ONLY_SER)


    //构Neightborhoodbc,key为节点，value为节点对应的邻居以及节点本身
    def getEgoGraphNodes(): RDD[(VertexId, Array[VertexId])]={
        return collectNeighbor.map(x =>(x._1,Array(x._1)++x._2))
    }
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
   
    var S = conductanceLocalMin()
    
    var kvalue=S.size
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
     G.unpersist()
     var (finalmatrix:DataFrame,value:Double)=Optimize(kvalue,n2c,trainrdd,testrdd,spark)

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
     val outputpath="/tmp/kq-youhua-pregel-k"+kvalue
     Com.flatMap{case(x,y) => y.map(c => (c,Array(x)))}.rdd.reduceByKey(aggfornodes).coalesce(100).map(x=>(x._1,x._2.mkString(","))).saveAsTextFile(outputpath)

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




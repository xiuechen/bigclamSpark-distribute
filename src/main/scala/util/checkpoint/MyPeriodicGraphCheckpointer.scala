package com.baidu.yundata.kg.util.checkpoint

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import com.baidu.yundata.kg.util.checkpoint.MyPeriodicCheckpointer


/**
 * This class helps with persisting and checkpointing Graphs.
 * Specifically, it automatically handles persisting and (optionally) checkpointing, as well as
 * unpersisting and removing checkpoint files.
 *
 * Users should call update() when a new graph has been created,
 * before the graph has been materialized.  After updating [[MyPeriodicGraphCheckpointer]], users are
 * responsible for materializing the graph to ensure that persisting and checkpointing actually
 * occur.
 *
 * When update() is called, this does the following:
 *  - Persist new graph (if not yet persisted), and put in queue of persisted graphs.
 *  - Unpersist graphs from queue until there are at most 3 persisted graphs.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new graph, and put in a queue of checkpointed graphs.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which Graphs should be
 *    checkpointed).
 *  - This class removes checkpoint files once later graphs have been checkpointed.
 *    However, references to the older graphs will still return isCheckpointed = true.
 *
 * Example usage:
 * {{{
 *  val (graph1, graph2, graph3, ...) = ...
 *  val cp = new MyPeriodicGraphCheckpointer(2, sc)
 *  cp.updateGraph(graph1)
 *  graph1.vertices.count(); graph1.edges.count()
 *  // persisted: graph1
 *  cp.updateGraph(graph2)
 *  graph2.vertices.count(); graph2.edges.count()
 *  // persisted: graph1, graph2
 *  // checkpointed: graph2
 *  cp.updateGraph(graph3)
 *  graph3.vertices.count(); graph3.edges.count()
 *  // persisted: graph1, graph2, graph3
 *  // checkpointed: graph2
 *  cp.updateGraph(graph4)
 *  graph4.vertices.count(); graph4.edges.count()
 *  // persisted: graph2, graph3, graph4
 *  // checkpointed: graph4
 *  cp.updateGraph(graph5)
 *  graph5.vertices.count(); graph5.edges.count()
 *  // persisted: graph3, graph4, graph5
 *  // checkpointed: graph4
 * }}}
 *
 * @param checkpointInterval Graphs will be checkpointed at this interval.
 *                           If this interval was set as -1, then checkpointing will be disabled.
 * @tparam VD  Vertex descriptor type
 * @tparam ED  Edge descriptor type
 *
 */
class MyPeriodicGraphCheckpointer[VD, ED](
    checkpointInterval: Int,
    sc: SparkContext)
  extends MyPeriodicCheckpointer[Graph[VD, ED]](checkpointInterval, sc) {

  override protected def checkpoint(data: Graph[VD, ED]): Unit = data.checkpoint()

  override protected def isCheckpointed(data: Graph[VD, ED]): Boolean = data.isCheckpointed

  override protected def persist(data: Graph[VD, ED]): Unit = {
    if (data.vertices.getStorageLevel == StorageLevel.NONE) {
      /* We need to use cache because persist does not honor the default storage level requested
       * when constructing the graph. Only cache does that.
       */
      data.vertices.cache()
    }
    if (data.edges.getStorageLevel == StorageLevel.NONE) {
      data.edges.cache()
    }
  }

  override protected def unpersist(data: Graph[VD, ED]): Unit = data.unpersist()

  override protected def getCheckpointFiles(data: Graph[VD, ED]): Iterable[String] = {
    data.getCheckpointFiles
  }
}

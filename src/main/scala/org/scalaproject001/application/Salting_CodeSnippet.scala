//To handle data skew in one of the geographical locations in Dataset A, especially when using .groupByKey() or
// .reduceByKey() in computeTopItemsPerLocation, we can use salting — a common Spark technique to mitigate skew
// by spreading the data more evenly across partitions.

import scala.util.Random

val saltedRDD = dedupedRdd
  .map { case (geoId, itemName) =>
    val salt = Random.nextInt(10) // salt range can be tuned
    ((geoId, salt, itemName), 1)
  }
  .reduceByKey(_ + _)
  .map { case ((geoId, _, itemName), count) =>
    ((geoId, itemName), count)
  }
  .reduceByKey(_ + _) // aggregate back across salts
  .map { case ((geoId, itemName), count) =>
    (geoId, (itemName, count))
  }
  .groupByKey()
  .mapValues { itemsIter =>
    val sortedItems = itemsIter.toSeq.sortBy { case (_, count) => -count }.take(topX)
    val warn = sortedItems.size < topX
    (sortedItems, warn)
  }

// Explanation:
//Salting (Random.nextInt(10)) splits one hot key (e.g., geoId=101) into 10 fake keys:
// ((101, 0), item), ((101, 1), item), ..., distributing load across partitions.
//After reduceByKey, we merge back to the real key geoId before ranking.
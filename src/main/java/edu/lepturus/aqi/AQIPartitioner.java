package edu.lepturus.aqi;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner，主要使用HashCode，加入城市名长度扰动
 *
 * @author T.lepturus
 * @version 1.0
 */
public class AQIPartitioner extends Partitioner<AQIKey, AQIIndexes> {
    @Override
    public int getPartition(AQIKey key, AQIIndexes value, int numPartitions) {
        return ((key.hashCode() & Integer.MAX_VALUE) + key.getCity().length()) % numPartitions;
    }
}

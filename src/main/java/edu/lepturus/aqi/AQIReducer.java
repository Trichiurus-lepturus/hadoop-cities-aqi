package edu.lepturus.aqi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer，统计每个城市每天的AQI
 *
 * @author T.lepturus
 * @version 1.0
 */
public class AQIReducer extends Reducer<AQIKey, AQIIndexes, Text, LongWritable> {
    @Override
    protected void reduce(AQIKey key, Iterable<AQIIndexes> values, Context context) throws IOException, InterruptedException {
        AQIIndexes indexes = new AQIIndexes();

        // 合并同座城市同天的数据
        for (AQIIndexes val : values) {
            indexes.merge(val);
        }

        double avgSO2 = indexes.getAvg(AQIIndexes.PollutionType.SO2);
        double avgNO2 = indexes.getAvg(AQIIndexes.PollutionType.NO2);
        double avgO3 = indexes.getAvg(AQIIndexes.PollutionType.O3);
        double avgPM10 = indexes.getAvg(AQIIndexes.PollutionType.PM10);
        double avgPM25 = indexes.getAvg(AQIIndexes.PollutionType.PM25);
        double avgCO = indexes.getAvg(AQIIndexes.PollutionType.CO);

        // 如果其中一项数据无效，则本城市本日AQI无效
        if (avgSO2 < 0 || avgNO2 < 0 || avgO3 < 0 || avgPM10 < 0 || avgPM25 < 0 || avgCO < 0) {
            context.write(
                    new Text(key.toString()),
                    new LongWritable(-1)
            );
        } else {
            context.write(
                    new Text(key.toString()),
                    new LongWritable(
                            AQIUtils.getAQIByParam(
                                    avgSO2,
                                    avgNO2,
                                    avgO3,
                                    avgPM10,
                                    avgPM25,
                                    avgCO,
                                    AQIUtils.DAQI
                            ).getAqi()
                    )
            );
        }
    }
}

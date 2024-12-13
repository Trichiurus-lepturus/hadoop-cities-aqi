package edu.lepturus.aqi;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * 本实验AQI计算需要用到的污染指数集合。
 *
 * @author T.lepturus
 * @version 1.0
 */
public class AQIIndexes implements Writable {
    /**
     * 污染指数之和
     * SO2 NO2 O3 PM10 PM25 CO
     */
    private final double[] sums = new double[6];

    /**
     * 污染指数数量
     * SO2 NO2 O3 PM10 PM25 CO
     */
    private final long[] counts = new long[6];

    public double[] getSums() {
        return sums;
    }

    public long[] getCounts() {
        return counts;
    }

    /**
     * 污染物类别
     */
    public enum PollutionType {
        SO2, NO2, O3, PM10, PM25, CO, UNDEFINED
    }

    /**
     * 将字符串解析为污染物类型
     *
     * @param string string
     * @return 污染物类型
     */
    public static AQIIndexes.PollutionType string2PollutionType(String string) {
        return switch (string) {
            case "SO2" -> AQIIndexes.PollutionType.SO2;
            case "NO2" -> AQIIndexes.PollutionType.NO2;
            case "O3" -> AQIIndexes.PollutionType.O3;
            case "PM10" -> AQIIndexes.PollutionType.PM10;
            case "PM2.5" -> AQIIndexes.PollutionType.PM25;
            case "CO" -> AQIIndexes.PollutionType.CO;
            default -> AQIIndexes.PollutionType.UNDEFINED;
        };
    }

    /**
     * 将污染物类型对应到数组下标
     *
     * @param type 污染物类型
     * @return 数组（this.sums, this.counts）下标
     */
    private int pollutionType2Index(PollutionType type) {
        return switch (type) {
            case SO2 -> 0;
            case NO2 -> 1;
            case O3 -> 2;
            case PM10 -> 3;
            case PM25 -> 4;
            case CO -> 5;
            default -> -1;
        };
    }

    /**
     * 增加统计一个数据
     *
     * @param type  污染物类型
     * @param value 污染指数
     */
    public void increase(PollutionType type, double value) {
        int index = pollutionType2Index(type);
        if (index >= 0) {
            ++counts[index];
            sums[index] += value;
        }
    }

    /**
     * 合并两份统计数据
     *
     * @param another another
     */
    public void merge(AQIIndexes another) {
        for (int i = 0; i < 6; ++i) {
            this.sums[i] += another.getSums()[i];
            this.counts[i] += another.getCounts()[i];
        }
    }

    /**
     * 获得某种污染物的平均污染指数
     *
     * @param type 污染物类型
     * @return 平均污染指数
     */
    public double getAvg(PollutionType type) {
        int index = pollutionType2Index(type);
        if (index >= 0 && counts[index] > 0) {
            return sums[index] / counts[index];
        }
        return -1; // 如果无效则返回-1
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(sums[0]);
        dataOutput.writeDouble(sums[1]);
        dataOutput.writeDouble(sums[2]);
        dataOutput.writeDouble(sums[3]);
        dataOutput.writeDouble(sums[4]);
        dataOutput.writeDouble(sums[5]);
        dataOutput.writeLong(counts[0]);
        dataOutput.writeLong(counts[1]);
        dataOutput.writeLong(counts[2]);
        dataOutput.writeLong(counts[3]);
        dataOutput.writeLong(counts[4]);
        dataOutput.writeLong(counts[5]);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sums[0] = dataInput.readDouble();
        sums[1] = dataInput.readDouble();
        sums[2] = dataInput.readDouble();
        sums[3] = dataInput.readDouble();
        sums[4] = dataInput.readDouble();
        sums[5] = dataInput.readDouble();
        counts[0] = dataInput.readLong();
        counts[1] = dataInput.readLong();
        counts[2] = dataInput.readLong();
        counts[3] = dataInput.readLong();
        counts[4] = dataInput.readLong();
        counts[5] = dataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AQIIndexes that = (AQIIndexes) o;
        return Objects.deepEquals(sums, that.sums) && Objects.deepEquals(counts, that.counts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(sums), Arrays.hashCode(counts));
    }
}

package edu.lepturus.aqi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;

/**
 * Mapper，每15行一组处理输入，生成AQIKey-AQIIndexes键值对
 *
 * @author T.lepturus
 * @version 1.0
 */
public class AQIMapper extends Mapper<LongWritable, Text, AQIKey, AQIIndexes> {
    /**
     * 批容量
     */
    private static final int BATCH_SIZE = 15;

    /**
     * 批缓存
     */
    private final List<String> buffer = new LinkedList<>();

    /**
     * 行计数器
     */
    private long lineCounter = 0;

    /**
     * CSV文件的列标题
     */
    private static String[] columnNames = null;

    @Override
    protected void setup(Mapper<LongWritable, Text, AQIKey, AQIIndexes>.Context context) throws IOException, InterruptedException {
        buffer.clear();
        lineCounter = 0;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            String line = value.toString();
            columnNames = line.split(",");
            return;
        }

        buffer.add(value.toString());
        ++lineCounter;

        if (lineCounter >= BATCH_SIZE) {
            doBatch(context);
            buffer.clear();
            lineCounter = 0;
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, AQIKey, AQIIndexes>.Context context) throws IOException, InterruptedException {
        if (!buffer.isEmpty()) {
            doBatch(context);
        }
        buffer.clear();
    }

    /**
     * 处理批
     *
     * @param context 上下文
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    void doBatch(Context context) throws IOException, InterruptedException {
        List<String[]> tokensLines = new ArrayList<>();
        for (String line : buffer) {
            String[] fields = line.split(",", -1);
            AQIIndexes.PollutionType type = AQIIndexes.string2PollutionType(fields[2]);
            if (fields.length >= columnNames.length && type != AQIIndexes.PollutionType.UNDEFINED) {
                tokensLines.add(fields);
            }
        }

        for (int colIndex = 3; colIndex < columnNames.length; ++colIndex) {
            try {
                AQIKey aqiKey = new AQIKey();
                aqiKey.setDate(new SimpleDateFormat("yyyyMMdd").parse(tokensLines.get(0)[0]));
                aqiKey.setCity(columnNames[colIndex]);
                AQIIndexes aqiIndexes = parseAQIIndexes(tokensLines, colIndex);
                context.write(aqiKey, aqiIndexes);
            } catch (ParseException ignored) {
            }
        }
    }

    /**
     * 解析某座城市某一日的污染指数集合
     *
     * @param tokensLines 按行字元
     * @param colIndex 列下标
     * @return AQIIndexes
     */
    private static AQIIndexes parseAQIIndexes(List<String[]> tokensLines, int colIndex) {
        AQIIndexes aqiIndexes = new AQIIndexes();

        for (String[] tokensLine : tokensLines) {
            if (colIndex < tokensLine.length) {
                try {
                    AQIIndexes.PollutionType type = AQIIndexes.string2PollutionType(tokensLine[2]);
                    double indexValue = Double.parseDouble(tokensLine[colIndex]);
                    aqiIndexes.increase(type, indexValue);
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return aqiIndexes;
    }
}

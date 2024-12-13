package edu.lepturus.aqi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * AQI计算：6项指标的24小时均值
 *
 * @author liyong
 * @version 1.0
 * @time 2024/11/13 上午10:56
 */
public class AQIUtils {

    /**
     * 空气质量分指数
     */
    private static List<Integer> AQIList = Arrays.asList(0, 50, 100, 150, 200, 300, 400, 500);
    /**
     * SO2 1小时 平均(ug/m3)
     */
    private static List<Integer> SO2HOURList = Arrays.asList(0, 150, 500, 650, 800);
    /**
     * SO2 24小时 平均
     */
    private static List<Integer> SO2DAYList = Arrays.asList(0, 50, 150, 475, 800, 1600, 2100, 2620);
    /**
     * NO2 1小时 平均(ug/m3)
     */
    private static List<Integer> NO2HOURList = Arrays.asList(0, 100, 200, 700, 1200, 2340, 3090, 3840);
    /**
     * NO2 24小时 平均
     */
    private static List<Integer> NO2DAYList = Arrays.asList(0, 40, 80, 180, 280, 565, 750, 940);
    /**
     * PM10 24小时 平均
     */
    private static List<Integer> PM10DAYList = Arrays.asList(0, 50, 150, 250, 350, 420, 500, 600);
    /**
     * CO 1小时 平均(mg/m3)
     */
    private static List<Integer> COHOURList = Arrays.asList(0, 5, 10, 35, 60, 90, 120, 150);
    /**
     * CO 24小时 平均(mg/m3)
     */
    private static List<Integer> CODAYList = Arrays.asList(0, 2, 4, 14, 24, 36, 48, 60);
    /**
     * O3 小时 平均(ug/m3)
     */
    private static List<Integer> O3HOURList = Arrays.asList(0, 160, 200, 300, 400, 800, 1000, 1200);
    /**
     * O3 8小时滑动 平均(ug/m3)
     */
    private static List<Integer> O3_8HList = Arrays.asList(0, 100, 160, 215, 265, 800);
    /**
     * PM25 24小时平均(ug/m3)
     */
    private static List<Integer> PM25DAYList = Arrays.asList(0, 35, 75, 115, 150, 250, 350, 500);

    private static List<String> listPollute = Arrays.asList("SO2", "NO2", "O3", "PM10", "PM25", "CO");

    private static final String SO2_TYPE = "SO2";
    private static final String NO2_TYPE = "NO2";
    private static final String O3_TYPE = "O3";
    private static final String PM25_TYPE = "PM25";
    private static final String PM10_TYPE = "PM10";
    private static final String CO_TYPE = "CO";

    public static final String HAQI = "H"; //小时aqi标识
    public static final String DAQI = "D"; //日aqi标识


    /**
     * 空气质量指数AQI的计算:供AQI计算调用
     * 注意事项:
     * 6参值顺序固定
     *
     * @param aqiType 静态变量HAQI:小时AQI    DAQI:日AQI
     *                参数值单位：CO为mg/m3  其它参数 ：μg/m3
     * @param SO2
     * @param NO2
     * @param O3
     * @param PM10
     * @param PM25
     * @param CO
     * @param aqiType
     * @return 　aqi (AQI值和首要污染物)
     */
    public static AQI getAQIByParam(Double SO2, Double NO2, Double O3, Double PM10, Double PM25, Double CO, String aqiType) {
        List<Integer> list = new ArrayList<>();
        AQI aqi = new AQI();
        // 二氧化硫（SO2 ）1 小时平均浓度值高于 800 μg/m3 的，
        // 不再进行其空气质量分指数计算，二氧化硫（SO2 ）空气质量分指数按 24 小时平均浓度计算的分指数报告。
        if (null != SO2 && SO2 > 0 && SO2 <= 800) {
            list.add(getIAQI(SO2_TYPE, SO2, aqiType));
        } else {
            list.add(null);
        }
        if (null != NO2 && NO2 > 0 && NO2 <= 3840) {
            list.add(getIAQI(NO2_TYPE, NO2, aqiType));
        } else {
            list.add(null);
        }
        if (null != O3 && O3 > 0 && O3 <= 1200) {
            list.add(getIAQI(O3_TYPE, O3, aqiType));
        } else {
            list.add(null);
        }
        if (null != PM10 && PM10 > 0 && PM10 <= 600) {//24小时
            list.add(getIAQI(PM10_TYPE, PM10, aqiType));
        } else {
            list.add(null);
        }
        if (null != PM25 && PM25 > 0 && PM25 <= 500) {//24小时
            list.add(getIAQI(PM25_TYPE, PM25, aqiType));
        } else {
            list.add(null);
        }
        if (null != CO && CO > 0 && CO <= 150) {
            list.add(getIAQI(CO_TYPE, CO, aqiType));
        } else {
            list.add(null);
        }
        // list长度可能小于listPollute长度；
        // 首要污染物为，空气质量指数大于 50 时，空气质量分指数最大的空气污染物。
        // 多种首要污染物
        if (null != list && list.size() > 0) {
            Integer maxAqiValue = null;
            for (Integer aqiValue : list) { // 全为null（返回null），部分为null，全不为null
                if (aqiValue != null) {
                    if (maxAqiValue == null) {
                        maxAqiValue = aqiValue;
                    } else {
                        if (aqiValue > maxAqiValue) {
                            maxAqiValue = aqiValue;
                        }
                    }
                }
            }
            //System.out.println(maxAqiValue);

            if (maxAqiValue != null && maxAqiValue > 0) {
                String pollutesJoint = null;
                for (int i = 0; i < list.size(); i++) {
                    Integer aqiValue = list.get(i);
                    if (maxAqiValue == aqiValue) { // 不会空指针异常
                        if (pollutesJoint == null) {
                            pollutesJoint = listPollute.get(i);
                        } else {
                            pollutesJoint += "," + listPollute.get(i);
                        }
                    }
                }
                aqi.setAqi(maxAqiValue);
                aqi.setName(pollutesJoint);
            }

        }
        return aqi;
    }

    /**
     * 分指数计算
     *
     * @param param
     * @param paramValue
     * @param type
     * @return
     */
    private static Integer getIAQI(String param, Double paramValue, String type) {
        int IAQI = -99;
        switch (param) {
            case SO2_TYPE:
                if (HAQI.equals(type)) {
                    IAQI = getAQIValue(SO2HOURList, paramValue);
                    break;
                } else {
                    IAQI = getAQIValue(SO2DAYList, paramValue);
                    break;
                }
            case NO2_TYPE:
                if (HAQI.equals(type)) {
                    IAQI = getAQIValue(NO2HOURList, paramValue);
                    break;
                } else {
                    IAQI = getAQIValue(NO2DAYList, paramValue);
                    break;
                }
            case O3_TYPE:
                if (HAQI.equals(type)) {
                    IAQI = getAQIValue(O3HOURList, paramValue);
                    break;
                } else {
                    IAQI = getAQIValue(O3_8HList, paramValue);
                    break;
                }
            case PM10_TYPE:
                IAQI = getAQIValue(PM10DAYList, paramValue);
                break;
            case PM25_TYPE:
                IAQI = getAQIValue(PM25DAYList, paramValue);
                break;
            case CO_TYPE:
                if (HAQI.equals(type)) {
                    IAQI = getAQIValue(COHOURList, paramValue);
                    break;
                } else {
                    IAQI = getAQIValue(CODAYList, paramValue);
                    break;
                }
        }
        return IAQI;
    }

    private static Integer getAQIValue(List<?> list, Double paramValue) {
        int IAQI = -99;
        int BPL = 0;//低位值
        int BPH = 0;//高位值
        int BPHIndex = 0;
        int IAQIH = 0;//BPH对应的空气质量分指数
        int IAQIL = 0;//BPL对应的空气质量分指数
        for (int i = 1; i < list.size(); i++) {
            if ((Integer) list.get(i) > Math.floor(paramValue)) {
                BPH = (Integer) list.get(i);
                BPL = (Integer) list.get(i - 1);
                BPHIndex = i;
                break;
            }
        }
        IAQIH = AQIList.get(BPHIndex);
        IAQIL = AQIList.get(BPHIndex - 1);
        Double AQI = ((((IAQIH - IAQIL) * 100) / (BPH - BPL)) * 0.01) * (paramValue - BPL) + IAQIL;
        if (null != AQI) {
            IAQI = (int) Math.ceil(AQI);
        }
        return IAQI;
    }


    /**
     * @param AQI aqi值
     * @return 质量水平
     */
    public static String getAQILevel(int AQI) {
        String level = "有毒害";
        if (AQI <= 50)
            level = "良好";
        else if (AQI <= 100)
            level = "中等";
        else if (AQI <= 150)
            level = "对敏感人群不健康";
        else if (AQI <= 200)
            level = "不健康";
        else if (AQI <= 300)
            level = "非常不健康";
        return level;
    }

//    public static void main(String[] args) throws Exception {
//        double SO2 = 15.0, NO2 = 27.0, O3 = 162.0, PM10 = 65.0, PM25 = 30.0, CO = 0.8;
//        AQI aqi = getAQIByParam(SO2, NO2, O3, PM10, PM25, CO, DAQI);
//        System.out.println(aqi);
//        //2.75 11.958333333333334 78.75 36.416666666666664 19.333333333333332
//        aqi = getAQIByParam(3.0, 15.0, 52.0, 31.0, 21.0, 0.53, DAQI);
//        System.out.println(aqi);
//
//    }

}

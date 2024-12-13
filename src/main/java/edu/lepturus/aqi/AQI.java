package edu.lepturus.aqi;

/**
 * AQI实体类
 *
 * @author liyong
 * @version 1.0
 * @time 2024/11/13 上午10:56
 */
public class AQI {
    /**
     * aqi值
     */
    protected int aqi;

    protected String name;

    public AQI() {
        super();
    }

    public AQI(int aqi, String name) {
        this.aqi = aqi;
        this.name = name;
    }

    public int getAqi() {
        return aqi;
    }

    public void setAqi(int aqi) {
        this.aqi = aqi;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name + "\t" + aqi;
    }
}

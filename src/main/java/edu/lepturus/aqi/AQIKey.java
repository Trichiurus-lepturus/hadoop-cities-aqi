package edu.lepturus.aqi;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * 键，城市名与日期的组合
 *
 * @author T.lepturus
 * @version 1.0
 */
public final class AQIKey implements WritableComparable<AQIKey> {
    private String city;
    private Date date;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(city);
        dataOutput.writeLong(date.getTime());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.city = dataInput.readUTF();
        this.date = new Date(dataInput.readLong());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AQIKey aqiKey = (AQIKey) o;
        return Objects.equals(city, aqiKey.city) && Objects.equals(date, aqiKey.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(city, date);
    }

    /**
     * 优先比较日期，而后比较城市
     *
     * @param another 比较对象
     * @return -1, 0, 1
     */
    @Override
    public int compareTo(AQIKey another) {
        int dateCompare = this.date.compareTo(another.date);
        if (dateCompare != 0) {
            return dateCompare;
        }
        return this.city.compareTo(another.city);
    }

    @Override
    public String toString() {
        return new SimpleDateFormat("yyyyMMdd").format(this.date) + "," + this.city;
    }
}


package com.products.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

public final class DateUtil {

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final int MILLISECONDS_OF_DAY = 24 * 60 * 60 * 1000;

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    public static final String SIMPLE_DATE_FORMAT = "yyyyMMdd";

    public static final String JS_DATE_FORMAT = "yyyy/MM/dd";

    public static String getQuarterEnd(final Date oDate) {
        int nYear;
        int nMonth;

        String strMM = null;

        final Calendar c = Calendar.getInstance();
        c.setTime(oDate);
        nYear = c.get(Calendar.YEAR);
        nMonth = c.get(Calendar.MONTH) + 1;
        if (nMonth <= 3) {
            strMM = "03";
        } else if (nMonth <= 6) {
            strMM = "06";
        } else if (nMonth <= 9) {
            strMM = "09";
        } else if (nMonth <= 12) {
            strMM = "12";
        }
        String sDate = nYear + "-" + strMM + "-" + "01";
        return getMonthEnd(parseDate(sDate));
    }


    public static String getQuarterStart(final Date oDate) {
        int nYear;
        int nMonth;

        String strMM = null;

        final Calendar c = Calendar.getInstance();
        c.setTime(oDate);
        nYear = c.get(Calendar.YEAR);
        nMonth = c.get(Calendar.MONTH) + 1;
        if (nMonth <= 3) {
            strMM = "01";
        } else if (nMonth <= 6) {
            strMM = "04";
        } else if (nMonth <= 9) {
            strMM = "07";
        } else if (nMonth <= 12) {
            strMM = "10";
        }
        String sDate = nYear + "-" + strMM + "-" + "01";
        return sDate;
    }

    public static Date getWeekStart(final Date oDate) {
        int dayOfWeek = getDayOfWeek(oDate);
        if (dayOfWeek == 0) {
        }
        return DateUtil.addDays(oDate, 1 - dayOfWeek);
    }


    public static Date getWeekEnd(final Date oDate) {
        int dayOfWeek = getDayOfWeek(oDate);
        if (dayOfWeek == 0) {
        }
        return DateUtil.addDays(oDate, 7 - dayOfWeek);
    }


    public static String getMonthEnd(final Date oDate) {
        int nYear;
        int nMonth;

        String strMM = null;
        String strDD = null;
        boolean leap = false;
        final Calendar c = Calendar.getInstance();
        c.setTime(oDate);
        nYear = c.get(Calendar.YEAR);
        nMonth = c.get(Calendar.MONTH) + 1;
        if (nMonth == 1 || nMonth == 3 || nMonth == 5 || nMonth == 7 || nMonth == 8 || nMonth == 10 || nMonth == 12) {
            strDD = "31";
        }
        if (nMonth == 4 || nMonth == 6 || nMonth == 9 || nMonth == 11) {
            strDD = "30";
        }
        if (nMonth == 2) {
            if (leap) {
                strDD = "29";
            } else {
                strDD = "28";
            }
        }
        strMM = nMonth >= 10 ? String.valueOf(nMonth) : ("0" + nMonth);
        return nYear + "-" + strMM + "-" + strDD;
    }

    public static boolean leapYear(final int year) {
        boolean leap;
        if (year % 4 == 0) {
            if (year % 100 == 0) {
                if (year % 400 == 0) {
                    leap = true;
                } else {
                    leap = false;
                }
            } else {
                leap = true;
            }
        } else {
            leap = false;
        }
        return leap;
    }

    public static Date addDays(final Date date, final int nDays) {
        if (null == date) {
            return date;
        }
        return new Date(date.getTime() + (long) nDays * MILLISECONDS_OF_DAY);
    }


    public static Date addHours(final Date date, final int nHours) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.HOUR, nHours);
        final Date end = c.getTime();
        return end;
    }


    public static Date addMilliseconds(final Date date, final int milliseconds) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MILLISECOND, milliseconds);
        final Date end = c.getTime();
        return end;
    }

    public static Date addMinutes(final Date date, final int minuts) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MINUTE, minuts);
        final Date end = c.getTime();
        return end;
    }


    public static Date addMonths(final Date date, final int nMonths) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.MONTH, nMonths);
        final Date end = c.getTime();
        return end;
    }


    public static Date addSeconds(final Date date, final int seconds) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.SECOND, seconds);
        final Date end = c.getTime();
        return end;
    }


    public static Date addYears(final Date date, final int nYears) {
        if (null == date) {
            return null;
        }
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.YEAR, nYears);
        final Date end = c.getTime();
        return end;
    }

    public static Date parserDateStr(String str, String format) {
        try {
            SimpleDateFormat f = new SimpleDateFormat(format);
            return f.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String dateTimeToString(final Date date, final String format) {
        if (date != null) {
            final SimpleDateFormat formatter = new SimpleDateFormat(format);
            return formatter.format(date);
        }
        return null;
    }

    public static Date parseDate(final String sDate) {
        Date date = parseDate(sDate, DEFAULT_DATE_FORMAT);
        if (null == date) {
            date = parseDate(sDate, "yyyyMMdd");
        }
        return date;
    }


    public static int getDayOfWeek(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
    }

    public static Date parseDate(final String sDate, final String format) {
        Date date = null;
        if (null != sDate && !"".equals(sDate)) {
            try {
                date = (Date) (new SimpleDateFormat(format)).parseObject(sDate);
            } catch (final ParseException e) {
                return date;

            }
        }
        return date;
    }

    public static String dateTimeToString(final Date date) {
        return dateTimeToString(date, DEFAULT_DATETIME_FORMAT);
    }


    public static String dateToString(final Date date) {
        return dateTimeToString(date, DEFAULT_DATE_FORMAT);
    }


    public static String dateToString(final Date date, final String format) {
        return dateTimeToString(date, format);
    }


    public static String dateToSimpleString(final Date date) {
        return dateTimeToString(date, SIMPLE_DATE_FORMAT);
    }

    public static String formatDate(Date date, String format) {
        SimpleDateFormat f = new SimpleDateFormat(format);
        return f.format(date);
    }


    public static int getDay(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.DAY_OF_MONTH);
    }


    public static int getYear(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.YEAR);
    }


    public static int getMonth(final Date date) {
        final Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.MONTH) + 1;
    }

    public static int getWeekOfDay(Date date) {
        GregorianCalendar canlendar = new GregorianCalendar();
        canlendar.setTime(date);
        return canlendar.get(GregorianCalendar.DAY_OF_WEEK);
    }

    public static Date getOneOfWeeks(final Date oDate, final int n) {
        final int nDay = ((n % 7) == 0) ? 7 : n % 7;

        int nDayOfWeek = DateUtil.getDayOfWeek(oDate);
        if (nDayOfWeek == 0) {
            nDayOfWeek = 7;
        }
        return addDays(oDate, nDay - nDayOfWeek);
    }

    public static Date makeDate(final int year, final int month, final int day) {
        final Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, day);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTime();
    }

    public static Date makeTime(final int hour, final int minute, final int second, final int milliSecond) {
        final Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 1900);
        c.set(Calendar.MONTH, 0);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, hour);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, milliSecond);
        return c.getTime();
    }

    public static Date makeDateTime(final Date date, final Date time) {
        final Calendar c = Calendar.getInstance();
        final Calendar t = Calendar.getInstance();

        if (null == date) {
            c.set(Calendar.YEAR, 1900);
            c.set(Calendar.MONTH, 0);
            c.set(Calendar.DAY_OF_MONTH, 1);
        } else {
            c.setTime(date);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }
        if (null == time) {
            t.set(Calendar.HOUR_OF_DAY, 0);
            t.set(Calendar.MINUTE, 0);
            t.set(Calendar.SECOND, 0);
            t.set(Calendar.MILLISECOND, 0);
        } else {
            t.setTime(time);
        }

        c.set(Calendar.HOUR_OF_DAY, t.get(Calendar.HOUR_OF_DAY));
        c.set(Calendar.MINUTE, t.get(Calendar.MINUTE));
        c.set(Calendar.SECOND, t.get(Calendar.SECOND));
        c.set(Calendar.MILLISECOND, t.get(Calendar.MILLISECOND));

        return c.getTime();
    }


    public static Date getMinDate(Date date1, Date date2) {
        return date1.before(date2) ? date1 : date2;
    }


    public static Date getMaxDate(Date date1, Date date2) {
        return date1.after(date2) ? date1 : date2;
    }


    public static String getCalendarDate(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        String monthStr = null;
        String dayStr = null;
        if (month < 10) {
            monthStr = "0" + month;
        } else {
            monthStr = String.valueOf(month);
        }
        if (day < 10) {
            dayStr = "0" + day;
        } else {
            dayStr = String.valueOf(day);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(year);
        sb.append("-");
        sb.append(monthStr);
        sb.append("-");
        sb.append(dayStr);
        return sb.toString();
    }


    public static LinkedList<Date> getConsecutiveDays(Date startTime,
                                                      Date endTime) {
        LinkedList<Date> result = new LinkedList<Date>();
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(startTime);

        while (!calendar.getTime().after(endTime)) {
            result.add(calendar.getTime());
            calendar.add(GregorianCalendar.DAY_OF_YEAR, 1);
        }
        return result;
    }


    public static int getDiffDays(Date startTime, Date endTime) {
        return getDiffDays(DateUtil.formatDate(startTime, "yyyyMMdd"),
                DateUtil.formatDate(endTime, "yyyyMMdd"), "yyyyMMdd");
    }

    public static int getDiffDays(String strStartDate, String strEndDate,
                                  String strFormat) {
        Date startTime = DateUtil.parserDateStr(strStartDate, strFormat);
        Date endTime = DateUtil.parserDateStr(strEndDate, strFormat);
        int flag = 1;
        if (startTime.after(endTime)) {
            flag = -1;
        }

        int days = 0;
        Calendar cBegin = Calendar.getInstance();
        Calendar cEnd = Calendar.getInstance();
        if (flag == 1) {
            cBegin.setTime(startTime);
            cEnd.setTime(endTime);
        } else {
            cBegin.setTime(endTime);
            cEnd.setTime(startTime);
        }

        while (cBegin.before(cEnd)) {
            days++;
            cBegin.add(Calendar.DAY_OF_YEAR, 1);
        }
        return (days + 1) * flag;
    }


    public static Date getThisQuarterStartDay(Date d) {
        GregorianCalendar canlendar = new GregorianCalendar();
        canlendar.setTime(d);
        int qmonth = 0;
        int year = canlendar.get(GregorianCalendar.YEAR);
        int month = canlendar.get(GregorianCalendar.MONTH);
        if (month >= 9) {
            qmonth = 9;
        } else if (month >= 6) {
            qmonth = 6;
        } else if (month >= 3) {
            qmonth = 3;
        }
        canlendar.set(GregorianCalendar.YEAR, year);
        canlendar.set(GregorianCalendar.MONTH, qmonth);
        canlendar.set(GregorianCalendar.DAY_OF_MONTH, 1);
        return canlendar.getTime();
    }


    public static Date getThisQuarterEndtDay(Date d) {
        GregorianCalendar canlendar = new GregorianCalendar();
        canlendar.setTime(d);
        int qmonth = 2;
        int days = 31;
        int year = canlendar.get(GregorianCalendar.YEAR);
        int month = canlendar.get(GregorianCalendar.MONTH);
        if (month >= 9) {
            qmonth = 11;
        } else if (month >= 6) {
            days = 30;
            qmonth = 8;
        } else if (month >= 3) {
            qmonth = 5;
            days = 30;
        }
        canlendar.set(GregorianCalendar.YEAR, year);
        canlendar.set(GregorianCalendar.MONTH, qmonth);
        canlendar.set(GregorianCalendar.DAY_OF_MONTH, days);
        return canlendar.getTime();
    }

    public static int getMinuteOfDay(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        int hourOfDay = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int minuteOfDay = hourOfDay * 60 + minute;
        return minuteOfDay;
    }

    public static int getHourOfDay(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    public static int getHourOfDay(final Date oDate) {
        if (null == oDate) {
            return -1;
        }

        final Calendar cal = Calendar.getInstance();
        cal.setTime(oDate);

        return cal.get(Calendar.HOUR_OF_DAY);
    }


    public static int getRemainMinutesInTheHour(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        int minute = cal.get(Calendar.MINUTE);
        return 60 - (minute + 1);
    }


    public static int getElapseMinutesInTheHour(long timeInMillis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timeInMillis);
        int minute = cal.get(Calendar.MINUTE);
        return minute + 1;
    }

    public static List<Date> getQuarterOfYearFirstDayAndLastDay(Date date, int lastQ) {
        List<Date> dList = new ArrayList<Date>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, lastQ * 3);
        int curYear = cal.get(Calendar.YEAR);
        int curMonth = cal.get(Calendar.MONTH);
        int qStartMonth = curMonth / 3 * 3;
        int qEndMonth = qStartMonth + 2;
        int endDay = 31;
        if (qEndMonth == 5 || qEndMonth == 8) {
            endDay = 30;
        }
        Calendar start = Calendar.getInstance();
        start.set(Calendar.YEAR, curYear);
        start.set(Calendar.MONTH, qStartMonth);
        start.set(Calendar.DAY_OF_MONTH, 1);
        start.set(Calendar.HOUR_OF_DAY, 0);
        start.set(Calendar.MINUTE, 0);
        start.set(Calendar.SECOND, 0);
        dList.add(start.getTime());
        Calendar end = Calendar.getInstance();
        end.set(Calendar.YEAR, curYear);
        end.set(Calendar.MONTH, qEndMonth);
        end.set(Calendar.DAY_OF_MONTH, endDay);
        end.set(Calendar.HOUR_OF_DAY, 0);
        end.set(Calendar.MINUTE, 0);
        end.set(Calendar.SECOND, 0);
        dList.add(end.getTime());
        return dList;
    }

    public static List<Date> getQuarterOfYearFirstDayAndLastDay(Date date) {
        return getQuarterOfYearFirstDayAndLastDay(date, 0);
    }

    public static String getQuarterOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int month = cal.get(Calendar.MONTH);
        int year = cal.get(Calendar.YEAR);
        if (month >= 0 && month <= 2) {
            return year + " Q1";
        }
        if (month >= 3 && month <= 5) {
            return year + " Q2";
        }
        if (month >= 6 && month <= 8) {
            return year + " Q3";
        }
        if (month >= 9 && month <= 11) {
            return year + " Q4";
        }
        return null;
    }

    public static int compareTo(Date theDate, Date otherdate) {
        Calendar one = Calendar.getInstance();
        one.setTime(getEndTimeOfDate(theDate));
        Calendar other = Calendar.getInstance();
        other.setTime(getEndTimeOfDate(otherdate));
        return one.compareTo(other);
    }

    public static Date getEndTimeOfDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar = getEndTimeOfDate(calendar);
        return calendar.getTime();
    }

    private static Calendar getEndTimeOfDate(Calendar calendar) {
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 1000);
        return calendar;
    }

    private DateUtil() {
    }

}

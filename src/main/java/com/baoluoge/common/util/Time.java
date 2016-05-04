package com.baoluoge.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by PaulZhang on 2016/5/4.
 */
public class Time {

    private final static Pattern p = Pattern.compile("(([0-9]+?)((d|h|mi|min|mn|s)))+?");
    private final static Integer MINUTE = 60;
    private final static Integer HOUR = 60 * MINUTE;
    private final static Integer DAY = 24 * HOUR;

    /**
     * Parse a duration
     *
     * @param duration 3h, 2mn, 7s or combination 2d4h10s, 1w2d3h10s
     * @return The number of seconds
     */
    public static int parseDuration(String duration) {
        if (duration == null) {
            return 30 * DAY;
        }

        final Matcher matcher = p.matcher(duration);
        int seconds = 0;
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid duration pattern : " + duration);
        }

        matcher.reset();
        while (matcher.find()) {
            if (matcher.group(3).equals("d")) {
                seconds += Integer.parseInt(matcher.group(2)) * DAY;
            } else if (matcher.group(3).equals("h")) {
                seconds += Integer.parseInt(matcher.group(2)) * HOUR;
            } else if (matcher.group(3).equals("mi") || matcher.group(3).equals("min") || matcher.group(3).equals("mn")) {
                seconds += Integer.parseInt(matcher.group(2)) * MINUTE;
            } else {
                seconds += Integer.parseInt(matcher.group(2));
            }
        }

        return seconds;
    }
}

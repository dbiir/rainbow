package cn.edu.ruc.iir.rainbow.benchmark.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.benchmark
 * @ClassName: DataUtil
 * @Description: To sum some static useful functions
 * @author: Tao
 * @date: Create in 2017-07-28 7:11
 **/
public class DataUtil
{

    /**
     * @ClassName: DataUtil
     * @Title:
     * @Description: judge "abc,ab," contains "abc" ?
     * @param:
     * @date: 7:15 2017/7/28
     */
    public static boolean isContainsStr(String str1, String str2)
    {
        boolean flag = false;
        str1 = str1.substring(0, str1.length() - 1);
        String[] str = str1.split(",");
        for (String s : str)
        {
            if (s.equals(str2))
            {
                flag = true;
                break;
            }
        }
        return flag;
    }

    public static synchronized String getCurTime()
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//set the style
        return df.format(new Date());
    }

}

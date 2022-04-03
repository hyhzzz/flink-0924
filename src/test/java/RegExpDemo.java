import java.util.Arrays;

/**
 * @author coderhyh
 * @create 2022-04-03 8:38
 */
class RegExpDemo {
    public static void main(String[] args) {

       /*

       Pattern 表示正则表达式。

       java在字符串提供了四个方法，直接支持正则表达式
       matches
       replaceall
       replacefirst
       split

正则语法：
    [ab] a或者b
    [a-z] 所有小写字母
    [a-zA-z] 所有字母都要
    [^ab] 不是a也不是b   ^放在方括号中表示非
    [a-zA-Z0-9_] 数字字母下划线   :简写 \\w :单词字符
    \d: [0-9]
    \D： 非数字
    \s: 空白字符：包含空格、\r \n \t
    .：表示任意字符，除了：\r \n
    \.：表示真正的点

    数量词：
        a{2}:正好两个a
        a{2,}:至少两个a
        a{2,5}:至少两个a 最多五个
        a+:至少一个a{1,}
        a*:执行一个a{0,}
        a?0个或1一个

        */

        //String s = "12";
        //boolean b = s.matches("\\d{2}");
        //System.out.println(b);


        //String s = "11abc@qq.com";
        //boolean b = s.matches("\\w{3,15}@\\w+\\.com|cn");
        //System.out.println(b);

        //String s = "aada2324312fdff";
        //System.out.println( s.replaceAll("\\d+","z"));


        // \1取第一组
        //String s=  "的打虎将爱我还低啊大大哇哒哒的的的黑";
        //System.out.println( s.replaceAll("(.)\\1+","$1"));


        //String s = "aaa13234345fdfvfddfdsc";
        //String[] data = s.split("\\D+");
        //System.out.println( Arrays.toString(data));


        String ip = "132,168,4.1";
        String[] data = ip.split("\\.");
        System.out.println(Arrays.toString(data));
    }
}

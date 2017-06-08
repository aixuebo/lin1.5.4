/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.filter.function;

import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.google.common.collect.ImmutableMap;

//内部支持的方法
public enum BuiltInMethod {
    UPPER(BuiltInMethod.class, "upper", String.class),
    LOWER(BuiltInMethod.class, "lower", String.class),
    SUBSTRING(BuiltInMethod.class, "substring", String.class, int.class, int.class),//字符串,开始位置,截取长度
    CHAR_LENGTH(BuiltInMethod.class, "charLength", String.class),//字符串长度,即执行String.length()
    LIKE(BuiltInMethod.class, "like", String.class, String.class),//使用正则表达式去匹配sql的like语法
    INITCAP(BuiltInMethod.class, "initcap", String.class);//oracal的函数,详细参见函数的实现类的备注

    public final Method method;

    public static final ImmutableMap<String, BuiltInMethod> MAP;//key是方法的name,value是内建方法对象

    static {
        final ImmutableMap.Builder<String, BuiltInMethod> builder = ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.name(), value);
            }
        }
        MAP = builder.build();
    }

    /**
     *
     * @param clazz 内部方法的具体实现类
     * @param methodName  执行该类的是方法
     * @param argumentTypes 该方法需要的参数集合
     */
    BuiltInMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this.method = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, argumentTypes);//反射找到该方法
    }

    /** SQL {@code LIKE} function. */
    public static boolean like(String s, String pattern) {
        if (s == null)
            return false;
        
        final String regex = Like.sqlToRegexLike(pattern, null);
        return Pattern.matches(regex, s);
    }

    /** SQL INITCAP(string) function.
     * oracal的函数
     * 返回一个字符串,将参数的字符串中每一个单词的第一个字母变成大写,其他字母变成小写.
     * 单词的拆分是按照空格或者非字母的字符作为拆分
     * 比如INITCAP('the soap') 返回The Soap
     * initcap("YEAR") 返回Year
     * initcap("YEAR 9index") 返回Year 9index
     * initcap("YEAR$$aaA")); 返回 Year$$Aaa 说明$$也被看成是空格,后面的A要变成大写
     **/
    public static String initcap(String s) {
        // Assumes Alpha as [A-Za-z0-9] 假设字母是 [A-Za-z0-9],其他非字母的都是空格
        // white space is treated as everything else.
        final int len = s.length();
        boolean start = true;//true说明已经开始查找了,即在查找第一个
        final StringBuilder newS = new StringBuilder();

        for (int i = 0; i < len; i++) {
            char curCh = s.charAt(i);
            final int c = (int) curCh;
            if (start) { // curCh is whitespace or first character of word. 说明是在查找第一个字母
                if (c > 47 && c < 58) { // 0-9
                    start = false;
                } else if (c > 64 && c < 91) { // A-Z
                    start = false;
                } else if (c > 96 && c < 123) { // a-z
                    start = false;
                    curCh = (char) (c - 32); // Uppercase this character 变成大写
                }
                // else {} whitespace
            } else { // Inside of a word or white space after end of word.//说明不是第一个字母
                if (c > 47 && c < 58) { // 0-9  数字不改变任何变化
                    // noop
                } else if (c > 64 && c < 91) { // A-Z
                    curCh = (char) (c + 32); // Lowercase this character 全部大写的字母变成小写字母
                } else if (c > 96 && c < 123) { // a-z
                    // noop
                } else { // whitespace
                    start = true;
                }
            }
            newS.append(curCh);//追加每一个元素
        } // for each character in s
        return newS.toString();
    }

    /** SQL CHARACTER_LENGTH(string) function. */
    public static int charLength(String s) {
        return s.length();
    }

    /** SQL SUBSTRING(string FROM ... FOR ...) function. */
    public static String substring(String s, int from, int for_) {
        if (s == null)
            return null;
        return s.substring(from - 1, Math.min(from - 1 + for_, s.length()));
    }

    /** SQL UPPER(string) function. */
    public static String upper(String s) {
        if (s == null)
            return null;
        return s.toUpperCase();
    }

    /** SQL LOWER(string) function. */
    public static String lower(String s) {
        if (s == null)
            return null;
        return s.toLowerCase();
    }

}
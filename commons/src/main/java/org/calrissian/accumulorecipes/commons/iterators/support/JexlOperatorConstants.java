/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.commons.iterators.support;

import org.apache.commons.jexl2.parser.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JexlOperatorConstants implements ParserTreeConstants {

    private static Map<Class<? extends JexlNode>, String> operatorMap = new ConcurrentHashMap<Class<? extends JexlNode>, String>();
    private static Map<String, Class<? extends JexlNode>> classMap = new ConcurrentHashMap<String, Class<? extends JexlNode>>();
    private static Map<Integer, String> jjtOperatorMap = new ConcurrentHashMap<Integer, String>();
    private static Map<String, Integer> jjtTypeMap = new ConcurrentHashMap<String, Integer>();

    static {
        operatorMap.put(ASTEQNode.class, "==");
        operatorMap.put(ASTNENode.class, "!=");
        operatorMap.put(ASTLTNode.class, "<");
        operatorMap.put(ASTLENode.class, "<=");
        operatorMap.put(ASTGTNode.class, ">");
        operatorMap.put(ASTGENode.class, ">=");
        operatorMap.put(ASTERNode.class, "=~");
        operatorMap.put(ASTNRNode.class, "!~");
        operatorMap.put(ASTFunctionNode.class, "f");
        operatorMap.put(ASTAndNode.class, "and");
        operatorMap.put(ASTOrNode.class, "or");

        classMap.put("==", ASTEQNode.class);
        classMap.put("!=", ASTNENode.class);
        classMap.put("<", ASTLTNode.class);
        classMap.put("<=", ASTLENode.class);
        classMap.put(">", ASTGTNode.class);
        classMap.put(">=", ASTGENode.class);
        classMap.put("=~", ASTERNode.class);
        classMap.put("!~", ASTNRNode.class);
        classMap.put("f", ASTFunctionNode.class);

        jjtOperatorMap.put(ParserTreeConstants.JJTEQNODE, "==");
        jjtOperatorMap.put(ParserTreeConstants.JJTNENODE, "!=");
        jjtOperatorMap.put(ParserTreeConstants.JJTLTNODE, "<");
        jjtOperatorMap.put(ParserTreeConstants.JJTLENODE, "<=");
        jjtOperatorMap.put(ParserTreeConstants.JJTGTNODE, ">");
        jjtOperatorMap.put(ParserTreeConstants.JJTGENODE, ">=");
        jjtOperatorMap.put(ParserTreeConstants.JJTERNODE, "=~");
        jjtOperatorMap.put(ParserTreeConstants.JJTNRNODE, "!~");
        jjtOperatorMap.put(ParserTreeConstants.JJTFUNCTIONNODE, "f");
        jjtOperatorMap.put(ParserTreeConstants.JJTANDNODE, "and");
        jjtOperatorMap.put(ParserTreeConstants.JJTORNODE, "or");

        jjtTypeMap.put("==", ParserTreeConstants.JJTEQNODE);
        jjtTypeMap.put("!=", ParserTreeConstants.JJTNENODE);
        jjtTypeMap.put("<", ParserTreeConstants.JJTLTNODE);
        jjtTypeMap.put("<=", ParserTreeConstants.JJTLENODE);
        jjtTypeMap.put(">", ParserTreeConstants.JJTGTNODE);
        jjtTypeMap.put(">=", ParserTreeConstants.JJTGENODE);
        jjtTypeMap.put("=~", ParserTreeConstants.JJTERNODE);
        jjtTypeMap.put("!~", ParserTreeConstants.JJTNRNODE);
        jjtTypeMap.put("f", ParserTreeConstants.JJTFUNCTIONNODE);

    }

    public static String getOperator(Class<? extends JexlNode> nodeType) {
        return operatorMap.get(nodeType);
    }

    public static String getOperator(Integer jjtNode) {
        return jjtOperatorMap.get(jjtNode);
    }

    public static Class<? extends JexlNode> getClass(String operator) {
        return classMap.get(operator);
    }

    public static int getJJTNodeType(String operator) {
        return jjtTypeMap.get(operator);
    }
}

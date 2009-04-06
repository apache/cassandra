/**
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

package org.apache.cassandra.continuations;

import java.lang.annotation.Annotation;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;
import org.apache.commons.javaflow.bytecode.transformation.ResourceTransformer;
import org.apache.commons.javaflow.bytecode.transformation.bcel.BcelClassTransformer;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class ContinuationClassTransformer implements ClassFileTransformer
{
    private static final String targetAnnotation_ = "Suspendable"; 
    private ResourceTransformer transformer_;    
    
    public ContinuationClassTransformer(String agentArguments, ResourceTransformer transformer)
    {
        super();
        transformer_ = transformer;        
    }
    
    public byte[] transform(ClassLoader classLoader, String className, Class redefiningClass, ProtectionDomain domain, byte[] bytes) throws IllegalClassFormatException
    {              
        /*
         * Use the ASM class reader to see which classes support
         * the Suspendable annotation. If they do then those 
         * classes need to have their bytecodes transformed for
         * Continuation support. 
        */                
        ClassReader classReader = new ClassReader(bytes);
        ClassNode classNode = new ClassNode();
        classReader.accept(classNode, true);       
        List<AnnotationNode> annotationNodes = classNode.visibleAnnotations;
        
        for( AnnotationNode annotationNode : annotationNodes )
        {            
            if (annotationNode.desc.contains(ContinuationClassTransformer.targetAnnotation_))
            {                
                bytes = transformer_.transform(bytes);
            }
        }                                
        return bytes;
    }
}

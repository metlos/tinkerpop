/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.junit.Test;

/**
 * @author Lukas Krejci
 */
public class DetachedTest {

    @Test
    public void testDetachedVertexIsComplete() {
        Vertex v = mockedVertex("id");

        DetachedVertex detached = DetachedFactory.detach(v, true);

        assertEquals("id", detached.id());
        assertEquals("label", detached.label());

        List<VertexProperty<Object>> props = collect(detached.properties());

        assertEquals(1, props.size());
        assertEquals("vpid", props.get(0).id());
        //DetachedVertexProperty doesn't have a separate key - it uses the label as its key...
        assertEquals("vplabel", props.get(0).label());
        assertEquals("vplabel", props.get(0).key());
        assertEquals("vpv", props.get(0).value());

        List<Property<Object>> metaProps = collect(props.get(0).properties());
        assertEquals(1, metaProps.size());
        assertEquals("p", metaProps.get(0).key());
        assertEquals("v", metaProps.get(0).value());
    }

    @Test
    public void testDetachedVertexIsSerializable() throws Exception {
        Vertex v = mockedVertex("id");

        DetachedVertex detached = DetachedFactory.detach(v, true);

        ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
        out.writeObject(detached);
    }

    @Test
    public void testDetachedEdgeIsComplete() {
        Edge edge = mockedEdge();

        DetachedEdge detached = DetachedFactory.detach(edge, true);

        assertEquals("id", detached.id());
        assertEquals("label", detached.label());
        assertEquals("source", detached.outVertex().id());
        assertEquals("target", detached.inVertex().id());

        List<Property<Object>> ps = collect(detached.properties());
        assertEquals(1, ps.size());

        Property<Object> p = ps.get(0);
        assertEquals("k", p.key());
        assertEquals("v", p.value());
    }

    @Test
    public void testDetachedEdgeIsSerializable() throws Exception {
        Edge edge = mockedEdge();

        DetachedEdge detached = DetachedFactory.detach(edge, true);

        ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
        out.writeObject(detached);
    }

    private Vertex mockedVertex(String id) {
        Graph g = mock(Graph.class);
        Graph.Features fs = mock(Graph.Features.class);
        Graph.Features.VertexFeatures vfs = mock(Graph.Features.VertexFeatures.class);

        when(g.features()).thenReturn(fs);
        when(fs.vertex()).thenReturn(vfs);
        when(vfs.supportsMetaProperties()).thenReturn(true);

        Vertex v = mock(Vertex.class);
        VertexProperty<Object> vp = mock(generic(VertexProperty.class));
        Property<Object> p = mock(generic(Property.class));

        when(p.key()).thenReturn("p");
        when(p.value()).thenReturn("v");
        when(p.isPresent()).thenReturn(true);
        when(p.element()).thenReturn(vp);

        when(vp.properties()).thenReturn(Collections.singleton(p).iterator());
        when(vp.id()).thenReturn("vpid");
        when(vp.label()).thenReturn("vplabel");
        when(vp.key()).thenReturn("vplabel");
        when(vp.value()).thenReturn("vpv");
        when(vp.element()).thenReturn(v);
        when(vp.graph()).thenReturn(g);

        when(v.id()).thenReturn(id);
        when(v.label()).thenReturn("label");
        when(v.properties()).thenReturn(Collections.singleton(vp).iterator());

        return v;
    }

    private Edge mockedEdge() {
        Vertex source = mockedVertex("source");
        Vertex target = mockedVertex("target");

        Edge edge = mock(Edge.class);

        when(edge.id()).thenReturn("id");
        when(edge.label()).thenReturn("label");
        when(edge.inVertex()).thenReturn(target);
        when(edge.outVertex()).thenReturn(source);
        when(edge.bothVertices()).thenReturn(Arrays.asList(source, target).iterator());
        when(edge.vertices(any())).thenAnswer(inv -> {
            Direction dir = (Direction) inv.getArguments()[0];
            switch (dir) {
                case IN:
                    return Collections.singleton(target).iterator();
                case OUT:
                    return Collections.singleton(source).iterator();
                case BOTH:
                    return Arrays.asList(source, target).iterator();
                default:
                    throw new AssertionError("Unhandled direction: " + dir);
            }
        });

        Property<Object> p = mock(generic(Property.class));
        when(p.key()).thenReturn("k");
        when(p.value()).thenReturn("v");
        when(p.isPresent()).thenReturn(true);
        when(p.element()).thenReturn(edge);

        when(edge.properties()).thenReturn(Collections.singleton(p).iterator());

        return edge;
    }

    @SuppressWarnings("unchecked")
    private <X> Class<X> generic(Class<?> rawClass) {
        return (Class<X>) rawClass;
    }

    private <T> List<T> collect(Iterator<T> it) {
        List<T> ret = new ArrayList<>();

        while (it.hasNext()) {
            ret.add(it.next());
        }

        return ret;
    }
}


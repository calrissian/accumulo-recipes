/*
 * Copyright (C) 2016 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumlorecipes.changelog.domain;

import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.codec.digest.DigestUtils.getMd5Digest;

/**
 * An internal hash node in a merkle tree that automatically derives the hashes of its children to form its own hash.
 */
public class HashNode implements Node {
    private static final long serialVersionUID = 1L;

    protected String hash;
    protected List<Node> children;

    public HashNode(List<Node> children) {
        this.children = children;

        MessageDigest digest = getMd5Digest();
        for (Node node : children)
            digest.update((node.getHash() + "\u0000").getBytes());

        hash = encodeHexString(digest.digest());
    }

    /**
     * Accessor for the children that this Node's hash is comprised of
     *
     * @return
     */
    @Override
    public List<Node> getChildren() {
        return unmodifiableList(children);
    }

    /**
     * If this node has children, aggregates the hashes of the children. If this node is a leaf, represents the hash
     * of the data.
     *
     * @return
     */
    @Override
    public String getHash() {
        return hash;
    }

    @Override
    public String toString() {
        return "HashNode{" +
                "hash=" + hash +
                ", children=" + children +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashNode hashNode = (HashNode) o;
        return Objects.equals(hash, hashNode.hash) &&
                Objects.equals(children, hashNode.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash, children);
    }
}
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

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.VectorIndexSearcher;
import org.apache.lucene.search.ScoreDoc;

import static org.assertj.core.api.Assertions.assertThat;

public class TopDocsPostingListTest
{
    @Test
    public void testNextPosting() throws IOException
    {
        VectorIndexSearcher.TopDocsPostingList postingList = createWithDocAndScores(1, 5.0, 2, 3.0, 3, 4.0, 4, 1.0);
        assertThat(values(postingList)).containsExactlyInAnyOrder(1, 2, 3, 4);

        postingList = createWithDocAndScores(6, 3.0, 5, 4.0, 4, 5.0, 3, 2.0);
        assertThat(values(postingList)).containsExactlyInAnyOrder(3, 4, 5, 6);
    }

    @Test
    public void testAdvance() throws IOException
    {
        VectorIndexSearcher.TopDocsPostingList postingList = createWithDocs(1, 2, 3, 4);
        postingList.advance(0); // no-op as first doc is greater than 0
        assertThat(values(postingList)).containsExactlyInAnyOrder(1, 2, 3, 4);

        postingList = createWithDocs(1, 3, 5, 7);
        postingList.advance(2);
        assertThat(values(postingList)).containsExactlyInAnyOrder(3, 5, 7);

        postingList = createWithDocs(1, 3, 5, 7);
        postingList.advance(2);
        postingList.advance(4);
        assertThat(postingList.nextPosting()).isEqualTo(5);
        postingList.advance(6);
        assertThat(postingList.nextPosting()).isEqualTo(7);
    }

    private static VectorIndexSearcher.TopDocsPostingList createWithDocs(long... docs)
    {
        ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        for (int i = 0; i < docs.length; i++)
        {
            float score = docs[i];
            scoreDocs[i] = new ScoreDoc(Math.toIntExact(docs[i]), score);
        }
        return new VectorIndexSearcher.TopDocsPostingList(scoreDocs);
    }

    private static VectorIndexSearcher.TopDocsPostingList createWithDocAndScores(Object... docAndScores)
    {
        ScoreDoc[] scoreDocs = new ScoreDoc[docAndScores.length / 2];
        for (int i = 0; i < docAndScores.length; i += 2)
        {
            int docId = (int) docAndScores[i];
            double score = (double) docAndScores[i + 1];
            scoreDocs[i / 2] = new ScoreDoc(docId, (float) score);
        }
        return new VectorIndexSearcher.TopDocsPostingList(scoreDocs);
    }

    private static List<Integer> values(PostingList postingList) throws IOException
    {
        List<Integer> values = new ArrayList<>();
        PostingList.PeekablePostingList peekable = postingList.peekable();

        while (peekable.peek() != PostingList.END_OF_STREAM)
            values.add(Math.toIntExact(peekable.nextPosting()));

        return values;
    }
}

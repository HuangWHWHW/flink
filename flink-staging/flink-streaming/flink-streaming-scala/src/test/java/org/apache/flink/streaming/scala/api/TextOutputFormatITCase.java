/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.api;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.scala.OutputFormatTestPrograms;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.test.testdata.WordCountData;

public class TextOutputFormatITCase extends StreamingProgramTestBase {

	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath);
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath, 1);
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath, null);
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath, FileSystem.WriteMode.OVERWRITE);
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath, null, 1);
		OutputFormatTestPrograms.wordCountToText(WordCountData.TEXT, resultPath, FileSystem.WriteMode.OVERWRITE, 1);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, resultPath);
	}

}

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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The {@link ConfigOption configuration options} relevant for all Executors.
 */
@PublicEvolving
public class DeploymentOptions {

	public static final ConfigOption<String> TARGET =
			key("execution.target")
					.stringType()
					.noDefaultValue()
					.withDescription("The deployment target for the execution, e.g. \"local\" for local execution.");

	// flink提供了两种基于yarn的提交模式，attached和detached。无论是jb和tm的提交还是任务的提交都支持这两种模式。
	// 和spark基于yarn的两种模式不同，flink的这两种模式仅仅只是在client端阻塞和非阻塞的区别，
	// attached模式会hold yarn的session直到任务执行结束，detached模式在提交完任务后就退出client
	public static final ConfigOption<Boolean> ATTACHED =
			key("execution.attached")
					.booleanType()
					.defaultValue(false)
					.withDescription("Specifies if the pipeline is submitted in attached or detached mode.");

	public static final ConfigOption<Boolean> SHUTDOWN_IF_ATTACHED =
			key("execution.shutdown-on-attached-exit")
					.booleanType()
					.defaultValue(false)
					.withDescription("If the job is submitted in attached mode, perform a best-effort cluster shutdown " +
							"when the CLI is terminated abruptly, e.g., in response to a user interrupt, such as typing Ctrl + C.");

	public static final ConfigOption<List<String>> JOB_LISTENERS =
			key("execution.job-listeners")
					.stringType()
					.asList()
					.noDefaultValue()
					.withDescription("Custom JobListeners to be registered with the execution environment." +
							" The registered listeners cannot have constructors with arguments.");
}

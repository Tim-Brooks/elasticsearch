/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
<<<<<<< HEAD
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
=======
>>>>>>> upstream/main

import javax.inject.Inject;

public abstract class GenerateNamedComponentsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateNamedComponentsTask.class);
    private static final String NAMED_COMPONENTS_DIR = "generated-named-components/";
    private static final String NAMED_COMPONENTS_FILE = "named_components.json";
    private static final String NAMED_COMPONENTS_PATH = NAMED_COMPONENTS_DIR + NAMED_COMPONENTS_FILE;

    private final WorkerExecutor workerExecutor;
    private FileCollection pluginScannerClasspath;
    private FileCollection classpath;
    private ExecOperations execOperations;
    private ProjectLayout projectLayout;

    @Inject
    public GenerateNamedComponentsTask(WorkerExecutor workerExecutor, ExecOperations execOperations, ProjectLayout projectLayout) {
        this.workerExecutor = workerExecutor;
        this.execOperations = execOperations;
        this.projectLayout = projectLayout;

        getOutputFile().convention(projectLayout.getBuildDirectory().file(NAMED_COMPONENTS_PATH));
    }

    @TaskAction
    public void scanPluginClasses() {
        File outputFile = projectLayout.getBuildDirectory().file(NAMED_COMPONENTS_PATH).get().getAsFile();

        ExecResult execResult = LoggedExec.javaexec(execOperations, spec -> {
            spec.classpath(pluginScannerClasspath.plus(getClasspath()).getAsPath());
            spec.getMainClass().set("org.elasticsearch.plugin.scanner.NamedComponentScanner");
            spec.args(outputFile);
            spec.setErrorOutput(System.err);
            spec.setStandardOutput(System.out);
        });
        execResult.assertNormalExitValue();
    }

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath.filter(File::exists);
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

<<<<<<< HEAD
    public abstract static class GenerateNamedComponentsAction implements WorkAction<Parameters> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void execute() {
            Set<File> classpathFiles = getParameters().getClasspath().getFiles();

            List<ClassReader> classReaders = ClassReaders.ofPaths(classpathFiles.stream().map(File::toPath));

            NamedComponentScanner namedComponentScanner = new NamedComponentScanner();
            Map<String, Map<String, String>> namedComponentsMap = namedComponentScanner.scanForNamedClasses(classReaders);
            writeToFile(namedComponentsMap);
        }

        private void writeToFile(Map<String, Map<String, String>> namedComponentsMap) {
            try {
                String json = OBJECT_MAPPER.writeValueAsString(namedComponentsMap);
                File file = getParameters().getOutputFile().getAsFile().get();
                Path of = Path.of(file.getAbsolutePath());
                Files.writeString(of, json);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
=======
    public void setPluginScannerClasspath(FileCollection pluginScannerClasspath) {
        this.pluginScannerClasspath = pluginScannerClasspath;
>>>>>>> upstream/main
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getPluginScannerClasspath() {
        return pluginScannerClasspath;
    }
}

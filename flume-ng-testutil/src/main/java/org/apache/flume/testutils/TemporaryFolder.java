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
package org.apache.flume.testutils;

import java.io.File;
import java.io.IOException;

public class TemporaryFolder {

  private final File parentFolder;

  public TemporaryFolder() throws IOException {
    parentFolder = createTemporaryFolderIn();
  }

  public void delete() {
    if (parentFolder != null) {
      recursiveDelete(parentFolder);
    }
  }

  private void recursiveDelete(File file) {
    File[] files = file.listFiles();
    if (files != null) {
      for (File each : files) {
        recursiveDelete(each);
      }
    }
    file.delete();
  }

  public File newFile(String fileName) throws IOException {
    File file = new File(getRoot(), fileName);
    if (!file.createNewFile()) {
      throw new IOException(
          "a file with the name \'" + fileName + "\' already exists in the test folder");
    }
    return file;
  }

  public File newFolder(String folder) throws IOException {
    return newFolder(new String[]{folder});
  }

  public File newFolder(String... folderNames) throws IOException {
    File file = getRoot();
    for (int i = 0; i < folderNames.length; i++) {
      String folderName = folderNames[i];
      validateFolderName(folderName);
      file = new File(file, folderName);
      if (!file.mkdir() && isLastElementInArray(i, folderNames)) {
        throw new IOException(
            "a folder with the name \'" + folderName + "\' already exists");
      }
    }
    return file;
  }

  private void validateFolderName(String folderName) throws IOException {
    File tempFile = new File(folderName);
    if (tempFile.getParent() != null) {
      String errorMsg = "Folder name cannot consist of multiple path components separated by a file separator."
          + " Please use newFolder('MyParentFolder','MyFolder') to create hierarchies of folders";
      throw new IOException(errorMsg);
    }
  }

  private boolean isLastElementInArray(int index, String[] array) {
    return index == array.length - 1;
  }

  private File getRoot() {
    if (parentFolder == null) {
      throw new IllegalStateException(
          "the temporary folder has not yet been created");
    }
    return parentFolder;
  }

  private File createTemporaryFolderIn() throws IOException {
    File createdFolder = File.createTempFile("embedded", "");
    createdFolder.delete();
    createdFolder.mkdir();
    return createdFolder;
  }
}

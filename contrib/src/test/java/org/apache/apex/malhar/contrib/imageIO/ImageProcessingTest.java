/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.imageIO;
/*
 * Tests for basic image processing operators.
 */
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;


public class ImageProcessingTest extends Resize
{
  private Data data = new Data();
  File imageFile;
  private static final Logger LOG = LoggerFactory.getLogger(ImageProcessingTest.class);

  @Before
  public void setup()
  {
    String soPath;
    String filePath;
    BufferedImage bufferedImage = null;
    imageFile = new File("src/test/resources/TestImages/TestImage.jpg");
    filePath = imageFile.getAbsolutePath();
    File soFile = new File("src/test/resources/libopencv_java320.so");
    soPath = soFile.getAbsolutePath();
    System.load(soPath);
    try {
      bufferedImageType = ImageIO.read(imageFile).getType();
      bufferedImage = ImageIO.read(imageFile);
    } catch (IOException e) {
      LOG.info("Error is " + e.getMessage());
    }
    data.bytesImage = bufferedImageToByteArray(bufferedImage);
    data.fileName = imageFile.getName();
    if (filePath.contains(".png")) {
      AbstractImageProcessingOperator.fileType = "png";
    }
    if (filePath.contains(".jpg")) {
      AbstractImageProcessingOperator.fileType = "jpg";
    }
    if (filePath.contains(".jpeg")) {
      AbstractImageProcessingOperator.fileType = "jpeg";
    }
    if (filePath.contains(".fits")) {
      AbstractImageProcessingOperator.fileType = "fits";
    }
    if (filePath.contains(".gif")) {
      AbstractImageProcessingOperator.fileType = "gif";
    }
    if (filePath.contains(".tif")) {
      AbstractImageProcessingOperator.fileType = "tif";
    }
  }

  @Test
  public void resizeTest()
  {
    ImageProcessingTest resizeTest = new ImageProcessingTest();
    resizeTest.scale = 0.5;
    BufferedImage original = byteArrayToBufferedImage(data.bytesImage);
    resizeTest.resize(data);
    BufferedImage result = byteArrayToBufferedImage(data.bytesImage);
    Boolean pass = false;
    if ((original.getWidth() * resizeTest.scale) == result.getWidth()) {
      pass = true;
    }
    Assert.assertEquals("Expectation", true, pass);
  }

  @Test
  public void compressTest()
  {
    Boolean pass = false;
    File compressedFile = new File("src/test/resources/TestImages/CompressedTestImage.jpg");
    Compress compress = new Compress();
    compress.compressionRatio = 0.9f;
    compress.compress(data);
    try {
      FileUtils.writeByteArrayToFile(compressedFile,data.bytesImage);
    } catch (IOException e) {
      LOG.debug(e.getMessage());
    }
    if (imageFile.length() > compressedFile.length() && compress.compressionRatio < 1f) {
      pass = true;
    }
    Assert.assertEquals("Expectation", true, pass);
  }

  @Test
  public void fileFormatConversionTest()
  {
    Boolean pass = false;
    FileFormatConverter fileFormatConverter = new FileFormatConverter();
    fileFormatConverter.toFileType = "png";
    fileFormatConverter.converter(data);
    if (data.fileName.contains(fileFormatConverter.toFileType) && !data.fileName.contains(AbstractImageProcessingOperator.fileType)) {
      pass = true;
    }
    Assert.assertEquals("Expectation", true, pass);
  }
}

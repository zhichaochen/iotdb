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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.common.Path;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 元数据工具类
 */
public class MetaUtils {

  private MetaUtils() {}

  /**
   * 将字符串路径切改成路数组路径
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedPath(String path) throws IllegalPathException {
    // NodeName is treated as identifier. When parsing identifier, unescapeJava is called.
    // Therefore we call unescapeJava here.
    path = StringEscapeUtils.unescapeJava(path);
    if (path.endsWith(TsFileConstant.PATH_SEPARATOR)) {
      throw new IllegalPathException(path);
    }
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    int endIndex;
    int length = path.length();
    for (int i = 0; i < length; i++) {
      if (path.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        String node = path.substring(startIndex, i);
        if (node.isEmpty()) {
          throw new IllegalPathException(path);
        }
        nodes.add(node);
        startIndex = i + 1;
      } else if (path.charAt(i) == TsFileConstant.BACK_QUOTE) {
        startIndex = i + 1;
        endIndex = path.indexOf(TsFileConstant.BACK_QUOTE, startIndex);
        if (endIndex == -1) {
          // single '`', like root.sg.`s
          throw new IllegalPathException(path);
        }
        while (endIndex != -1 && endIndex != length - 1) {
          char afterQuote = path.charAt(endIndex + 1);
          if (afterQuote == TsFileConstant.BACK_QUOTE) {
            // for example, root.sg.```
            if (endIndex == length - 2) {
              throw new IllegalPathException(path);
            }
            endIndex = path.indexOf(TsFileConstant.BACK_QUOTE, endIndex + 2);
          } else if (afterQuote == '.') {
            break;
          } else {
            throw new IllegalPathException(path);
          }
        }
        // replace `` with ` in a quoted identifier
        String node = path.substring(startIndex, endIndex).replace("``", "`");
        if (node.isEmpty()) {
          throw new IllegalPathException(path);
        }

        nodes.add(node);
        // skip the '.' after '`'
        i = endIndex + 1;
        startIndex = endIndex + 2;
      }
    }
    // last node
    if (startIndex <= path.length() - 1) {
      String node = path.substring(startIndex);
      if (node.isEmpty()) {
        throw new IllegalPathException(path);
      }
      nodes.add(node);
    }
    return nodes.toArray(new String[0]);
  }

  /**
   * Get storage group path when creating schema automatically is enable
   *
   * <p>e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  public static PartialPath getStorageGroupPathByLevel(PartialPath path, int level)
      throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path.getFullPath());
    }
    String[] storageGroupNodes = new String[level + 1];
    System.arraycopy(nodeNames, 0, storageGroupNodes, 0, level + 1);
    return new PartialPath(storageGroupNodes);
  }

  /**
   * PartialPath of aligned time series will be organized to one AlignedPath. BEFORE this method,
   * all the aligned time series is NOT united. For example, given root.sg.d1.vector1[s1] and
   * root.sg.d1.vector1[s2], they will be organized to root.sg.d1.vector1 [s1,s2]
   *
   * @param fullPaths full path list without uniting the sub measurement under the same aligned time
   *     series. The list has been sorted by the alphabetical order, so all the aligned time series
   *     of one device has already been placed contiguously.
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     vector1 (s1,s2) would be returned once.
   */
  public static List<PartialPath> groupAlignedPaths(List<PartialPath> fullPaths) {
    List<PartialPath> result = new LinkedList<>();
    AlignedPath alignedPath = null;
    for (PartialPath path : fullPaths) {
      MeasurementPath measurementPath = (MeasurementPath) path;
      if (!measurementPath.isUnderAlignedEntity()) {
        result.add(measurementPath);
        alignedPath = null;
      } else {
        if (alignedPath == null || !alignedPath.equals(measurementPath.getDeviceIdString())) {
          alignedPath = new AlignedPath(measurementPath);
          result.add(alignedPath);
        } else {
          alignedPath.addMeasurement(measurementPath);
        }
      }
    }
    return result;
  }

  @TestOnly
  public static List<String> getMultiFullPaths(IMNode node) {
    if (node == null) {
      return Collections.emptyList();
    }

    List<IMNode> lastNodeList = new ArrayList<>();
    collectLastNode(node, lastNodeList);

    List<String> result = new ArrayList<>();
    for (IMNode lastNode : lastNodeList) {
      result.add(lastNode.getFullPath());
    }

    return result;
  }

  @TestOnly
  public static void collectLastNode(IMNode node, List<IMNode> lastNodeList) {
    if (node != null) {
      Map<String, IMNode> children = node.getChildren();
      if (children.isEmpty()) {
        lastNodeList.add(node);
      }

      for (Entry<String, IMNode> entry : children.entrySet()) {
        IMNode childNode = entry.getValue();
        collectLastNode(childNode, lastNodeList);
      }
    }
  }

  /**
   * Merge same series and convert to series map. For example: Given: paths: s1, s2, s3, s1 and
   * aggregations: count, sum, count, sum. Then: pathToAggrIndexesMap: s1 -> 0, 3; s2 -> 1; s3 -> 2
   *
   * @param selectedPaths selected series
   * @return path to aggregation indexes map
   */
  public static Map<PartialPath, List<Integer>> groupAggregationsBySeries(
      List<? extends Path> selectedPaths) {
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap = new HashMap<>();
    for (int i = 0; i < selectedPaths.size(); i++) {
      PartialPath series = (PartialPath) selectedPaths.get(i);
      pathToAggrIndexesMap.computeIfAbsent(series, key -> new ArrayList<>()).add(i);
    }
    return pathToAggrIndexesMap;
  }

  /**
   * Group all the series under an aligned entity into one AlignedPath and remove these series from
   * pathToAggrIndexesMap. For example, input map: d1[s1] -> [1, 3], d1[s2] -> [2,4], will return
   * d1[s1,s2], [[1,3], [2,4]]
   */
  public static Map<AlignedPath, List<List<Integer>>> groupAlignedSeriesWithAggregations(
      Map<PartialPath, List<Integer>> pathToAggrIndexesMap) {
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap = new HashMap<>();
    Map<String, AlignedPath> temp = new HashMap<>();
    List<PartialPath> seriesPaths = new ArrayList<>(pathToAggrIndexesMap.keySet());
    for (PartialPath seriesPath : seriesPaths) {
      // for with value filter
      if (seriesPath instanceof AlignedPath) {
        List<Integer> indexes = pathToAggrIndexesMap.remove(seriesPath);
        AlignedPath groupPath = temp.get(seriesPath.getFullPath());
        if (groupPath == null) {
          groupPath = (AlignedPath) seriesPath.copy();
          temp.put(groupPath.getFullPath(), groupPath);
          alignedPathToAggrIndexesMap
              .computeIfAbsent(groupPath, key -> new ArrayList<>())
              .add(indexes);
        } else {
          // groupPath is changed here so we update it
          List<List<Integer>> subIndexes = alignedPathToAggrIndexesMap.remove(groupPath);
          subIndexes.add(indexes);
          groupPath.addMeasurements(((AlignedPath) seriesPath).getMeasurementList());
          groupPath.addSchemas(((AlignedPath) seriesPath).getSchemaList());
          alignedPathToAggrIndexesMap.put(groupPath, subIndexes);
        }
      } else if (((MeasurementPath) seriesPath).isUnderAlignedEntity()) {
        // for without value filter
        List<Integer> indexes = pathToAggrIndexesMap.remove(seriesPath);
        AlignedPath groupPath = temp.get(seriesPath.getDeviceIdString());
        if (groupPath == null) {
          groupPath = new AlignedPath((MeasurementPath) seriesPath);
          temp.put(seriesPath.getDeviceIdString(), groupPath);
          alignedPathToAggrIndexesMap
              .computeIfAbsent(groupPath, key -> new ArrayList<>())
              .add(indexes);
        } else {
          // groupPath is changed here so we update it
          List<List<Integer>> subIndexes = alignedPathToAggrIndexesMap.remove(groupPath);
          subIndexes.add(indexes);
          groupPath.addMeasurement((MeasurementPath) seriesPath);
          alignedPathToAggrIndexesMap.put(groupPath, subIndexes);
        }
      }
    }
    return alignedPathToAggrIndexesMap;
  }
}

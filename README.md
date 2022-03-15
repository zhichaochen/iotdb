<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->



# Compaction Policy Tuning Extension
This is an extension of IoTDB, which implements the compaction policy tuning module. To use the module, several parameters of IoTDB would be used. The introduction of IoTDB can be found [here](https://github.com/apache/iotdb.git).

```aidl
enable_separation_tuning=true   # to enable the tuning algorithm
delay_num = 10000               # the size of delay set to analyze
total_capacity=512              # the capacity of the MemTable
```

The tuning server should be started before the database, which is  "separation_py/SeparationServer.py". The default IP is "127.0.0.1", and the default port is "8989".

When running IoTDB, a tuning client would be started, which is org.apache.iotdb.db.separation.statistics.SeparationTuningClient. It will constantly send the collected delay samples to the server and tune the compaction policy accordingly.
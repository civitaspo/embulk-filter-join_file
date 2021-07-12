# Join File filter plugin for Embulk

This plugin combine rows from file having data format like a table, based on a common field between them.

## Overview

* **Plugin type**: filter

## Configuration

* **on**:
  * **in_column**: name of the column on input. (string, required)
  * **file_column**: name of the column on file. (string, default is the same as **in_column**)
* **file**:
  * **path**: path of file (string, required)
  * **format**: file format (string, required, supported: `json`)
  * **encode**: file encode (string, default is `raw`, supported: `raw`, `gzip`)
  * **columns**: required columns of data from the file (array of hash, required)
    * **name**: name of the column
    * **type**: type of the column (see below)
    * **format**: format of the timestamp if type is timestamp
    * **timezone**: timezone of the timestamp if type is timestamp  

---
**type of the column**

|name|description|
|:---|:---|
|boolean|true or false|
|long|64-bit signed integers|
|timestamp|Date and time with nano-seconds precision|
|double|64-bit floating point numbers|
|string|Strings|
|json|JSON|

## Example

```yaml
filters:
  - type: join_file
    on:
      in_column: name_id
      file_column: id
    file:
      path: ./master.json
      format: json
      encode: raw
      columns:
        - {name: id, type: long}
        - {name: name, type: string}
    joined_column_prefix: _joined_by_embulk_
```

## Run Example

```
$ ./gradlew classpath
$ embulk run -I lib example/config.yml
```

## Supported Data Format
* json

### Supported Data Format Example

#### JSON

```
[
  {
    "id": 0,
    "name": "civitaspo"
  },
  {
    "id": 2,
    "name": "moriogai"
  },
  {
    "id": 5,
    "name": "natsume.soseki"
  }
]
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

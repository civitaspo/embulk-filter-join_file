# Join File filter plugin for Embulk

This plugin combine rows from file having data format like a table, based on a common field between them.

## Overview

* **Plugin type**: filter

## Configuration

- **base_column**: a column name of data embulk loaded (hash, required)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp
- **counter_column**: a column name of data loaded from file (string, default: `{name: id, type: long}`)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp
- **joined_keys_prefix**: prefix added to joined data keys (string, default: `"_joined_by_embulk_"`)
- **file_path**: path of file (string, required)
- **file_format**: file format (string, required, supported: `csv`, `tsv`, `yaml`, `json`)
- **columns**: required columns of json table (array of hash, required)
  - **name**: name of the column
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp

---
**type of the column**

|name|description|
|:---|:---|
|boolean|true or false|
|long|64-bit signed integers|
|timestamp|Date and time with nano-seconds precision|
|double|64-bit floating point numbers|
|string|Strings|

## Example

```yaml
filters:
  - type: left_outer_join_json_table
    base_column: {name: name_id, type: long}
    counter_column: {name: id, type: long}
    joined_keys_prefix: _joined_by_embulk_
    file_path: master.json
    file_format: json
    columns:
      - {name: id, type: long}
      - {name: name, type: string}
```

## Supported Data Format
- csv ( **not implemented** )
- tsv ( **not implemented** )
- yaml ( **not implemented** )
- json

### Supported Data Format Example

#### CSV

```csv
id,name
0,civitaspo
2,mori.ogai
5,natsume.soseki
```

#### TSV

Since the representation is difficult, it represents the tab as `\t`.

```tsv
id\tname
0\tcivitaspo
2\tmori.ogai
5\tnatsume.soseki
```

#### YAML

```
- id: 0
  name: civitaspo
- id: 2
  name: mori.ogai
- id: 5
  name: natsume.soseki
```

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

## Run Example

```
$ ./gradlew classpath
$ embulk run -I lib example/config.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

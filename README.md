# Join File filter plugin for Embulk

This plugin combine rows from file having data format like a table, based on a common field between them.

## Overview

* **Plugin type**: filter

## Configuration

* **on**:
  * **page_column**: name of the column on page. (string, required)
  * **file_column**: name of the column on file. (string, required)
* **file**:
  * **path_prefix**: Path prefix of input files (string, required)
  * **parser**: Parser configurations (see below [Supported Parser Type](#supported-parser-type)) (hash, required)
  * **decoders**: Decoder configuration (see below [Supported Decorder Type](#supported-decorder-type)) (array of hash, optional)
  * **follow_symlinks**: If true, follow symbolic link directories (boolean, default: `false`)
  * **columns**: required columns of data from the file (array of hash, required)
    * **name**: name of the column
    * **type**: type of the column (see below [Type of the column](#type-of-the-column))
    * **format**: format of the timestamp if type is timestamp
    * **timezone**: timezone of the timestamp if type is timestamp
  * **join_table_column_prefix**: prefix added to join table column name for prevent duplicating column name (string, default: `"_join_by_embulk_""`)

### Supported Parser Type

* You can use all embulk file-parser plugins.
  * [built-in parser plugins](http://www.embulk.org/docs/built-in.html)
  * [parser plugins](http://www.embulk.org/plugins/#file-parser).
* Special Configuration which [embulk-filter-join_file](./) can set in **parser** section.
  * **columns_option_name**: Set the **file.columns** value to the option which this option indicates. (optional, default: `"columns"`)
  * **join_file_columns_option_name**: Same as the **columns_option_name** option. Use this if a parser plugin has **columns_option_name** as its owned option.

### Supported Decorder Type

* You can use all embulk file-decorder plugins.
  * [built-in decorder plugins](http://www.embulk.org/docs/built-in.html)
  * [decorder plugins](http://www.embulk.org/plugins/#file-decoder)

### Type of the column

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
      page_column: id
      file_column: id
    file:
      path_prefix: ./example/json_array_of_hash/*.json
      parser:
        type: jsonpath
        root: "$."
      columns:
        - {name: id, type: long}
        - {name: name, type: string}
        - {name: created_at, type: timestamp, format: "%Y-%m-%d"}
        - {name: point, type: double}
        - {name: time_zone, type: string}
      join_table_column_prefix: _join_by_embulk_
```

See [more examples](./example).

## Run Example

```
$ ./gradlew classpath
$ embulk bundle install --gemfile=example/Gemfile --path vendor/bundle
$ embulk run -b example -Ilib example/config.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

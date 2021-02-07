![Build](https://github.com/ShadowItaly/membuffer/workflows/Build/badge.svg)
<a href="https://crates.io/crates/membuffer" rel="nofollow">
![value](https://img.shields.io/crates/v/membuffer)
</a>
# membuffer
A rust library for rapid deserialization of huge datasets with few keys. The library is meant to be used with mmaped files, almost any crate on crates.io which does serialization and deserialization needs to process the whole structure. This makes it unusable with large memory mapped files. For this purpose this library only scans the header to get the schema of the datastructure and leaves all other fields untouched unless it is specifically asked to fetch them.

# Examples

```rust
use membuffer::{MemBufferWriter,MemBufferReader,MemBufferError};

fn main() {
  //Creates a new empty MemBufferWriter
  let mut writer = MemBufferWriter::new();
  
  //Adds this as immutable field, no more changing after adding it
  writer.add_string_entry("short_key","short_value");

  //Creates a Vec<u8> out of all the collected data
  let result = writer.finalize();

  //Try to read the created vector. Will return an error if the CRC32 does not fit
  //or if the header is not terminated. Will panic if the memory is corrupted beyond recognition
  let reader = MemBufferReader::new(&result).unwrap();

  //Will return an error if the selected key could not be found or if the value types dont match
  assert_eq!(reader.get_string_field("short_key").unwrap(), "short_value");
}
```

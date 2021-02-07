[![Build](https://github.com/ShadowItaly/membuffer/workflows/Build/badge.svg)](https://github.com/ShadowItaly/membuffer/actions)
[![value](https://img.shields.io/crates/v/membuffer)](https://crates.io/crates/membuffer)

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

# Benchmark
[Benchmark](assets/benchmark.png)
Why is the library this fast? The benchmark consists of deserializing a data structure with different payload sizes either 1 MB, 10 MB or 100 MB. The membuffer load only the data structure layout and returns a slice to the strings instead of parsing the whole structure. This will help heaps if one uses MMAPed structures for example. As one can see in the benchmarks the speed of membuffer is only dependent on the number of keys and not of the size of the datastructure deserialized which is a good proof that the complexity of the deserialization does not depend on the size of the datastructure.

**Benchmark code:**
```rust
#[bench]
fn benchmark_few_keys_payload_1mb_times_3(b: &mut Bencher) {
  let mut huge_string = String::with_capacity(10_000_000);
  for _ in 0..1_000_000 {
    huge_string.push('a');
  }
  let mut writer = MemBufferWriter::new();
  writer.add_string_entry("one",&huge_string);
  writer.add_string_entry("two",&huge_string);
  writer.add_string_entry("three",&huge_string);
  let result = writer.finalize();
  assert!(result.len() > 3_000_000);

  b.iter(|| {
      let reader = MemBufferReader::new(&result).unwrap();
      let string1 = reader.get_string_field("one").unwrap();
      let string2 = reader.get_string_field("two").unwrap();
      let string3 = reader.get_string_field("three").unwrap();
      assert_eq!(string1.len(), 1_000_000);
      assert_eq!(string2.len(), 1_000_000);
      assert_eq!(string3.len(), 1_000_000);
      });
}

#[bench]
fn benchmark_few_keys_payload_1mb_times_3_serde(b: &mut Bencher) {
  let mut huge_string = String::with_capacity(1_000_000);
  for _ in 0..1_000_000 {
    huge_string.push('a');
  }
  let first = BenchSerde {
one: &huge_string,
       two: &huge_string,
       three: &huge_string
  };

  let string = serde_json::to_string(&first).unwrap();

  b.iter(|| {
      let reader: BenchSerde = serde_json::from_str(&string).unwrap();
      assert_eq!(reader.one.len(), 1_000_000);
      assert_eq!(reader.two.len(), 1_000_000);
      assert_eq!(reader.three.len(), 1_000_000);
      });
}
```

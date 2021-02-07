#![cfg_attr(feature = "bench", feature(test))]
extern crate test;
use byteorder::{WriteBytesExt, ReadBytesExt, BigEndian};
use crc32fast::Hasher;
use serde::{Serialize,Deserialize};
use serde_json;

///Refers to a position of a datafield
pub struct Position {
    pub offset: i32,
    pub length: i32,
    pub variable_type: i32,
}

///The Writer class which sets up the schema and writes it into the memory when finished building
pub struct MemBufferWriter {
    offsets: std::collections::HashMap<String,Position>,
    data: Vec<u8>
}

///The reader which is used for reading the memory area produced by the writer
pub struct MemBufferReader<'a> {
    offsets: std::collections::HashMap<&'a str,Position>,
    data: &'a [u8]
}

///The types which can be stored in the memory format writer at the moment
#[derive(Debug,Clone)]
pub enum Type {
    Integer32,
    Text,
    Vecu8,
    Vecu16,
    Vecu32,
    Vecu64
}

impl ToString for Type {
    fn to_string(&self) -> String {
        match self {
            Type::Integer32 => String::from("32-bit Integer"),
            Type::Text => String::from("String"),
            Type::Vecu8 => String::from("Vec<u8>"),
            Type::Vecu16 => String::from("Vec<u16>"),
            Type::Vecu32 => String::from("Vec<u32>"),
            Type::Vecu64 => String::from("Vec<u64>"),
        }
    }
}



#[derive(Debug, Clone)]
pub enum MemBufferError {
    FieldUnknown(String),
    FieldTypeError(Type,Type),
    Crc32Wrong,
    WrongFormat,
}

impl<'a> std::fmt::Display for MemBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MemBufferError::FieldUnknown(x) => write!(f, "Memory buffer error: Field {} unknown",x),
            MemBufferError::FieldTypeError(x,y) => write!(f,"Memory buffer error: Field has type {:?} and not requested type {}",x.to_string(),y.to_string()),
            MemBufferError::Crc32Wrong => write!(f,"Memory buffer error: The crc32 does not match with the given header, memory corrupted!"),
            MemBufferError::WrongFormat => write!(f,"Memory buffer error: Reached end of slice before end of header, memory seems to be corrupted")

        }
    }
}


impl<'a> MemBufferReader<'a> {
    ///Deserialize data from a buffer to an i32 integer
    fn deserialize_i32_from(mut buffer: &[u8]) -> i32 {
        buffer.read_i32::<BigEndian>().unwrap()
    }

    ///Deserialize a string from the given memory slice
    fn deserialize_string_from(buffer: &'a [u8]) -> Result<&'a str,MemBufferError> {
        let size = (MemBufferReader::deserialize_i32_from(buffer)+4) as usize;
        if size > buffer.len() {
            return Err(MemBufferError::WrongFormat);
        }
        Ok(unsafe{ std::str::from_utf8_unchecked(&buffer[4..size]) })
    }

    pub fn get_serde_field<'de, T: Deserialize<'de>>(&'de self, name: &'a str) -> Result<T,serde_json::Error> {
        let val = self.get_string_field(name).unwrap();
        let des: T = serde_json::from_str(val)?;
        Ok(des)
    }

    ///Tries to get the given field as string
    pub fn get_string_field(&'a self, name: &'a str) -> Result<&'a str,MemBufferError> {
        if let Some(pos) = self.offsets.get(name) {
            unsafe {
                return Ok(std::str::from_utf8_unchecked(&self.data[(pos.offset as usize)..((pos.offset+pos.length) as usize)]));
            }
        }
        Err(MemBufferError::FieldUnknown(name.to_string()))
    }

    pub fn get_vecu64_field(&'a self, name: &'a str) -> Result<Vec<u64>, MemBufferError> {
        if let Some(pos) = self.offsets.get(name) {
            let mut result = vec![0; pos.length as usize];
            (&self.data[(pos.offset as usize)..(pos.offset as usize+((pos.length as usize)*std::mem::size_of::<u64>()) as usize)]).read_u64_into::<BigEndian>(&mut result).unwrap();
            return Ok(result);
        }
        Err(MemBufferError::FieldUnknown(name.to_string()))
    }

    pub fn get_i32_field(&'a self, name: &'a str) -> Result<i32,MemBufferError> {
        if let Some(pos) = self.offsets.get(name) {
            return Ok(MemBufferReader::deserialize_i32_from(&self.data[(pos.offset as usize)..((pos.offset+pos.length) as usize)]));
        }
        Err(MemBufferError::FieldUnknown(name.to_string()))
    }

    ///Creates a new memory format reader from the given memory slice, as the readed values are
    ///borrowed from the memory slice the reader cannot outlive the memory it borrows from
    pub fn new(val: &'a [u8]) -> Result<MemBufferReader<'a>,MemBufferError> {
        let mut current_slice = &val[..];
        let mut offsets: std::collections::HashMap<&str,Position> = std::collections::HashMap::new();
        let mut start = 0;
        if val.len() < 8 {
            return Err(MemBufferError::WrongFormat);
        }

        loop {
            let positions_offset = MemBufferReader::deserialize_i32_from(current_slice);
            if positions_offset == 0x7AFECAFE {
                break;
            }

            let positions_length = MemBufferReader::deserialize_i32_from(&current_slice[4..]);
            let positions_type = MemBufferReader::deserialize_i32_from(&current_slice[8..]);
            let key = MemBufferReader::deserialize_string_from(&current_slice[12..])?;

            let key_length = key.len();
            start += 16+key_length;
            if (start+3) >= val.len() {
            }
            current_slice = &current_slice[(16+key_length)..];
            offsets.insert(key,Position {
                offset: positions_offset,
                length: positions_length,
                variable_type: positions_type,
            });
        }
        start+=4;

        let mut crc32hasher = Hasher::new();
        crc32hasher.update(&val[0..start]);
        let crc = MemBufferReader::deserialize_i32_from(&current_slice[4..8]);
        if (crc as u32) != crc32hasher.finalize() {
            return Err(MemBufferError::Crc32Wrong);
        }

        Ok(MemBufferReader {
            offsets,
            data: &current_slice[8..]
        })
    }
}

impl<'a> std::fmt::Debug for MemBufferReader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"Found memory buffer with payload size {}",self.data.len())
    }
}

impl MemBufferWriter {
    ///Creates a new empty memory format writer
    pub fn new() -> MemBufferWriter {
        MemBufferWriter {
            offsets: std::collections::HashMap::new(),
            data: Vec::new()
        }
    }

    ///Serializes the integer to the memory slice
    fn serialize_i32_to(val: i32, to: &mut Vec<u8>) {
        to.write_i32::<BigEndian>(val).unwrap();
    }

    ///Serializes the string to the memory slice
    fn serialize_string_to(string: &str, to: &mut Vec<u8>) {
        MemBufferWriter::serialize_i32_to(string.len() as i32, to);
        to.extend_from_slice(string.as_bytes());
    }


    pub fn add_serde_entry<T: Serialize>(&mut self, name: &str, content: &T) {
        self.add_string_entry(name,&serde_json::to_string(&content).unwrap());
    }


    ///Adds a new string entry to the serializer
    pub fn add_string_entry(&mut self, name: &str, content: &str) {
        if let Some(_) = self.offsets.get(name) {
            panic!("There is already a field with the name {}",name);
        }
        let offset = self.data.len();
        let len = content.len();
        self.data.extend_from_slice(&content.as_bytes());
        self.offsets.insert(name.to_string(), Position {
            offset: offset as i32,
            length: len as i32,
            variable_type: Type::Text as i32,
        });
    }

    ///Adds a new i32 entry to the serializer
    pub fn add_i32_entry(&mut self, name: &str, content: i32) {
        if let Some(_) = self.offsets.get(name) {
            panic!("There is already a field with the name {}",name);
        }
        let offset = self.data.len();
        self.data.write_i32::<BigEndian>(content).unwrap();
        self.offsets.insert(name.to_string(), Position {
            offset: offset as i32,
            length: 4,
            variable_type: Type::Integer32 as i32,
        });
    }

    ///Adds a new vector to the serializer
    pub fn add_vec_u8_entry(&mut self, name: &str, content: &[u8]) {
        if let Some(_) = self.offsets.get(name) {
            panic!("There is already a field with the name {}",name);
        }
        let offset = self.data.len();
        self.data.extend_from_slice(content);
        self.offsets.insert(name.to_string(), Position {
            offset: offset as i32,
            length: std::mem::size_of::<i32>() as i32,
            variable_type: Type::Vecu8 as i32,
        });
    }


    ///Adds a u64 vector to the entry
    pub fn add_vec_u64_entry(&mut self, name: &str, content: &[u64]) {
        if let Some(_) = self.offsets.get(name) {
            panic!("There is already a field with the name {}",name);
        }
        let offset = self.data.len();
        let len = content.len();
        for elem in content {
            self.data.write_u64::<BigEndian>(*elem).unwrap();
        }
        self.offsets.insert(name.to_string(), Position {
            offset: offset as i32,
            length: len as i32,
            variable_type: Type::Vecu64 as i32,
        });
    }

    ///Finalize the schema and return the memory slice holding the whole vector
    pub fn finalize(&self) -> Vec<u8> {
        let mut var: Vec<u8> = Vec::with_capacity(self.data.len()+self.offsets.len()*20);
        for (key, val) in self.offsets.iter() {
            MemBufferWriter::serialize_i32_to(val.offset, &mut var);
            MemBufferWriter::serialize_i32_to(val.length, &mut var);
            MemBufferWriter::serialize_i32_to(val.variable_type, &mut var);
            MemBufferWriter::serialize_string_to(&key, &mut var);
        }
        MemBufferWriter::serialize_i32_to(0x7AFECAFE, &mut var);
        let mut crc = Hasher::new();
        crc.update(&var);
        MemBufferWriter::serialize_i32_to(crc.finalize() as i32, &mut var);
        var.extend_from_slice(&self.data);
        return var;
    }
}



#[cfg(test)]
mod tests {
    use super::{MemBufferWriter,MemBufferReader,MemBufferError};
    use serde::{Serialize,Deserialize};
    use serde_json;

    #[derive(Serialize,Deserialize)]
    struct HeavyStruct {
        vec: Vec<u64>,
        name: String,
        frequency: i32,
        id: i32,
    }
    
    #[test]
    fn check_serde_capability() {
        let value = HeavyStruct {
            vec: vec![100,20,1],
            name: String::from("membuffer!"),
            frequency: 10,
            id: 200,
        };
        let mut writer = MemBufferWriter::new();
        writer.add_serde_entry("heavy", &value);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        let struc: HeavyStruct = reader.get_serde_field("heavy").unwrap();

        assert_eq!(struc.vec, vec![100,20,1]);
        assert_eq!(struc.name,"membuffer!");
        assert_eq!(struc.frequency,10);
        assert_eq!(struc.id,200);
    }

    #[test]
    fn check_serialize_string_deserialize() {
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("world","Earth");
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.get_string_field("world").unwrap(), "Earth");
    }

    #[test]
    fn check_mem_shift() {
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("world","Earth");
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result[1..]);
        assert_eq!(reader.is_err(),true);
    }

    #[test]
    fn check_serialize_u64_deserialize() {
        let mut writer = MemBufferWriter::new();
        let val:Vec<u64> = vec![10,100,1000];
        writer.add_vec_u64_entry("nice", &val);
        writer.add_string_entry("world","Earth");
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result);
        assert_eq!(reader.is_ok(),true);
        let membuffer = reader.unwrap();

        assert_eq!(membuffer.get_string_field("world").unwrap(), "Earth");
        assert_eq!(membuffer.get_vecu64_field("nice").unwrap(), val);
    }

    #[test]
    fn check_mem_crc() {
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("world","Earth");
        let mut result = writer.finalize();
        result[17] = 'b' as u8;

        let reader = MemBufferReader::new(&result[0..]);
        assert_eq!(reader.is_err(),true);
        let error = reader.unwrap_err();

        match error {
            MemBufferError::Crc32Wrong => assert!(true),
            _ => assert!(false,"The error was not CRC32"),
        }
    }

    #[test]
    fn check_serialize_i32_deserialize() {
        let mut writer = MemBufferWriter::new();
        writer.add_i32_entry("id",123);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.get_i32_field("id").unwrap(), 123);
    }
    use test::Bencher;

    #[bench]
    fn benchmark_few_keys_payload_1mb(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(1_000_000);
        for _ in 0..1_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("nice",&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.get_string_field("nice").unwrap();
            assert_eq!(string.len(), 1_000_000);
        });
    }

    #[bench]
    fn benchmark_few_keys_payload_10mb(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(10_000_000);
        for _ in 0..10_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("nice",&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.get_string_field("nice").unwrap();
            assert_eq!(string.len(), 10_000_000);
        });
    }

    #[bench]
    fn benchmark_few_keys_payload_100mb(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(10_000_000);
        for _ in 0..100_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("nice",&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.get_string_field("nice").unwrap();
            assert_eq!(string.len(), 100_000_000);
        });
    }

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
    fn benchmark_few_keys_payload_100mb_times_3(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(100_000_000);
        for _ in 0..100_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_string_entry("one",&huge_string);
        writer.add_string_entry("two",&huge_string);
        writer.add_string_entry("three",&huge_string);
        let result = writer.finalize();
        assert!(result.len() > 300_000_000);

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string1 = reader.get_string_field("one").unwrap();
            let string2 = reader.get_string_field("two").unwrap();
            let string3 = reader.get_string_field("three").unwrap();
            assert_eq!(string1.len(), 100_000_000);
            assert_eq!(string2.len(), 100_000_000);
            assert_eq!(string3.len(), 100_000_000);
        });
    }

    #[derive(Serialize,Deserialize)]
    struct BenchSerde<'a> {
        one: &'a str,
        two: &'a str,
        three: &'a str
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
}

#![cfg_attr(feature = "bench", feature(test))]

#[cfg(feature = "bench")]
extern crate test;


use byteorder::{WriteBytesExt, ReadBytesExt, NativeEndian};
use serde::{Serialize,Deserialize};
use serde_json;


///!This crate will provide a extremely fast deserialization of dynamic data structures with big
///fields. This is very MMAP friendly since it only parses the header and does not parse the fields
///until requested.
///**Easy example:**
///```rust
///use membuffer::{MemBufferWriter,MemBufferReader};
///
///fn main() {
///  //Creates a new empty MemBufferWriter
///  let mut writer = MemBufferWriter::new();
///  
///  //Adds this as immutable field, no more changing after adding it
///  writer.add_entry("Very long value");
///
///  //Creates a Vec<u8> out of all the collected data
///  let result = writer.finalize();
///
///  //Try to read the created vector. Will return an error if the CRC32 does not fit
///  //or if the header is not terminated. Will panic if the memory is corrupted beyond recognition
///  let reader = MemBufferReader::new(&result).unwrap();
///
///  //Will return an error if the selected key could not be found or if the value types dont match
///  assert_eq!(reader.load_entry::<&str>(0).unwrap(), "Very long value");
///}
///```

///Refers to a position of a datafield
pub struct Position {
    pub offset: i32,
    pub length: i32,
}

pub enum MemBufferTypes {
    Text,
    Integer32,
    VectorU8,
    VectorU64,
    LastPreDefienedValue
}

struct InternPosition {
    pub pos: Position,
    pub variable_type: i32,
}




#[derive(Debug, Clone)]
pub enum MemBufferError {
    FieldUnknown(String),
    FieldTypeError(i32,i32),
    Crc32Wrong,
    WrongFormat,
}

impl<'a> std::fmt::Display for MemBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MemBufferError::FieldUnknown(x) => write!(f, "Memory buffer error: Field {} unknown",x),
            MemBufferError::FieldTypeError(x,y) => write!(f,"Memory buffer error: Field has type {} and not requested type {}",x.to_string(),y.to_string()),
            MemBufferError::Crc32Wrong => write!(f,"Memory buffer error: The crc32 does not match with the given header, memory corrupted!"),
            MemBufferError::WrongFormat => write!(f,"Memory buffer error: Reached end of slice before end of header, memory seems to be corrupted")

        }
    }
}


pub trait MemBufferDeserialize<'a,T> {
    fn from_mem_buffer(pos: &Position, mem: &'a [u8]) -> Result<T,MemBufferError> where Self: Sized;
}

impl<'a> MemBufferDeserialize<'a,&'a str> for &str {
    fn from_mem_buffer(pos: &Position, mem: &'a [u8]) -> Result<&'a str,MemBufferError> {
        unsafe{ Ok(std::str::from_utf8_unchecked(&mem[pos.offset as usize..(pos.offset+pos.length) as usize])) }
    }
}

impl<'a> MemBufferDeserialize<'a,i32> for i32 {
    fn from_mem_buffer(pos: &Position, _: &'a [u8]) -> Result<i32,MemBufferError> {
        Ok(pos.offset)
    }
}

impl<'a> MemBufferDeserialize<'a,&'a [u8]> for &[u8] {
    fn from_mem_buffer(pos: &Position, mem: &'a [u8]) -> Result<&'a [u8],MemBufferError> {
        Ok(&mem[pos.offset as usize..(pos.offset+pos.length) as usize])
    }
}

impl<'a> MemBufferDeserialize<'a,&'a [u64]> for &[u64] {
    fn from_mem_buffer(pos: &Position, mem: &'a [u8]) -> Result<&'a [u64],MemBufferError> {
        let val: *const u8 = mem[pos.offset as usize..].as_ptr();
        let cast_memory = val.cast::<u64>();
        //Divide by eight as u64 should be 8 bytes on any system
        let mem_length = pos.length>>3;

        Ok(unsafe{std::slice::from_raw_parts(cast_memory, mem_length as usize)})
    }
}

///The reader which is used for reading the memory area produced by the writer
pub struct MemBufferReader<'a> {
    offsets: Vec<InternPosition>,
    data: &'a [u8]
}

impl<'a> MemBufferReader<'a> {
    ///Deserialize data from a buffer to an i32 integer
    fn deserialize_i32_from(mut buffer: &[u8]) -> i32 {
        buffer.read_i32::<NativeEndian>().unwrap()
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn payload_len(&self) -> usize {
        self.data.len()
    }

    pub fn load_entry<X: MemBufferDeserialize<'a,X> + MemBufferSerialize>(&self,index: usize) -> Result<X,MemBufferError> {
        let expected_type = X::get_mem_buffer_type();
        let is_type = self.offsets[index].variable_type;
        if is_type != expected_type {
            return Err(MemBufferError::FieldTypeError(is_type,expected_type));
        }
        X::from_mem_buffer(&self.offsets[index].pos, self.data)
    }

    pub fn load_serde_entry<T: Deserialize<'a>>(&self,index: usize) -> Result<T,MemBufferError> {
        let string = self.load_entry::<&str>(index)?;
        Ok(serde_json::from_str(string).unwrap())
    }


    ///Creates a new memory format reader from the given memory slice, as the readed values are
    ///borrowed from the memory slice the reader cannot outlive the memory it borrows from
    pub fn new(val: &'a [u8]) -> Result<MemBufferReader<'a>,MemBufferError> {
        let mut current_slice = &val[..];
        let mut offsets: Vec<InternPosition> = Vec::new();

        if val.len() < 16 {
            return Err(MemBufferError::WrongFormat);
        }

        loop {
            let positions_offset = MemBufferReader::deserialize_i32_from(current_slice);
            if positions_offset == 0x7AFECAFE {
                break;
            }
            
            if current_slice.len() < 16 {
                return Err(MemBufferError::WrongFormat);
            }

            let positions_length = MemBufferReader::deserialize_i32_from(&current_slice[4..]);
            let positions_type = MemBufferReader::deserialize_i32_from(&current_slice[8..]);

            current_slice = &current_slice[12..];
            offsets.push(InternPosition {
                pos: Position {
                offset: positions_offset,
                length: positions_length
                },
                variable_type: positions_type,
            });
        }

        Ok(MemBufferReader {
            offsets,
            data: &current_slice[4..]
        })
    }
}

impl<'a> std::fmt::Debug for MemBufferReader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"Found memory buffer with payload size {}",self.data.len())
    }
}


///The Writer class which sets up the schema and writes it into the memory when finished building
pub struct MemBufferWriter {
    offsets: Vec<InternPosition>,
    data: Vec<u8>
}

pub trait MemBufferSerialize {
    fn to_mem_buffer<'a>(&'a self, offset: i32) -> (Position,&'a [u8]);
    fn get_mem_buffer_type() -> i32; 
}

impl MemBufferSerialize for &str {
    fn to_mem_buffer<'a>(&'a self, offset: i32) -> (Position, &'a [u8]) {
        (Position {
            offset,
            length: self.len() as i32},self.as_bytes())
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Text as i32
    }
}

impl MemBufferSerialize for &String {
    fn to_mem_buffer<'a>(&'a self, offset: i32) -> (Position, &'a [u8]) {
        (Position {
            offset,
            length: self.len() as i32},self.as_bytes())
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Text as i32
    }
}

impl MemBufferSerialize for i32 {
    fn to_mem_buffer<'a>(&'a self, _: i32) -> (Position, &'a [u8]) {
        (Position {
            offset: *self,
            length: 0},&[])
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Integer32 as i32
    }
}

impl MemBufferSerialize for &[u8] {
    fn to_mem_buffer<'a>(&'a self, offset: i32) -> (Position, &'a [u8]) {
        (Position {
            offset,
            length: self.len() as i32},self)
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::VectorU8 as i32
    }
}

impl MemBufferSerialize for &[u64] {
    fn to_mem_buffer<'a>(&'a self, offset: i32) -> (Position, &'a [u8]) {
        let val: *const u64 = self.as_ptr();
        let cast_memory = val.cast::<u8>();
        let mem_length = self.len() * std::mem::size_of::<u64>();
        println!("Memory length: {}",mem_length);

        (Position {
            offset,
            length: mem_length as i32},unsafe{ std::slice::from_raw_parts(cast_memory, mem_length)})
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::VectorU64 as i32
    }
}

impl MemBufferWriter {
    ///Creates a new empty memory format writer
    pub fn new() -> MemBufferWriter {
        MemBufferWriter {
            offsets: Vec::new(),
            data: Vec::new()
        }
    }

    ///Serializes the integer to the memory slice
    fn serialize_i32_to(val: i32, to: &mut Vec<u8>) {
        to.write_i32::<NativeEndian>(val).unwrap();
    }

    pub fn add_entry<T: MemBufferSerialize>(&mut self, val: T) {
        let (pos,slice) = val.to_mem_buffer(self.data.len() as i32);
        self.offsets.push(InternPosition{pos,variable_type: T::get_mem_buffer_type()});
        self.data.extend_from_slice(slice);
    }

    pub fn add_serde_entry<T: Serialize>(&mut self, val: &T) {
        let as_str = serde_json::to_string(val).unwrap();
        self.add_entry::<&str>(&as_str);
    }


    ///Finalize the schema and return the memory slice holding the whole vector
    pub fn finalize(&self) -> Vec<u8> {
        let mut var: Vec<u8> = Vec::with_capacity(self.data.len()+self.offsets.len()*20);
        for val in self.offsets.iter() {
            MemBufferWriter::serialize_i32_to(val.pos.offset, &mut var);
            MemBufferWriter::serialize_i32_to(val.pos.length, &mut var);
            MemBufferWriter::serialize_i32_to(val.variable_type, &mut var);
        }
        MemBufferWriter::serialize_i32_to(0x7AFECAFE, &mut var);
        var.extend_from_slice(&self.data);
        return var;
    }
}



#[cfg(test)]
mod tests {
    use super::{MemBufferWriter,MemBufferReader};
    use serde::{Serialize,Deserialize};

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
        writer.add_serde_entry(&value);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        let struc: HeavyStruct = reader.load_serde_entry(0).unwrap();

        assert_eq!(struc.vec, vec![100,20,1]);
        assert_eq!(struc.name,"membuffer!");
        assert_eq!(struc.frequency,10);
        assert_eq!(struc.id,200);
    }

    #[test]
    fn check_serialize_string_deserialize() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("Earth");
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&str>(0).unwrap(), "Earth");
    }

    #[test]
    fn check_serialize_vecu8_deserialize() {
        let mut writer = MemBufferWriter::new();
        let some_bytes : Vec<u8> = vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10];
        writer.add_entry(&some_bytes[..]);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&[u8]>(0).unwrap(), vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10]);
    }

    #[test]
    fn check_serialize_vecu64_deserialize() {
        let mut writer = MemBufferWriter::new();
        let some_bytes : Vec<u64> = vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10];
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&[u64]>(0).unwrap(), vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10]);
        assert_eq!(reader.load_entry::<&[u64]>(1).unwrap(), vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10]);
    }

    #[test]
    fn check_len() {
        let mut writer = MemBufferWriter::new();
        let some_bytes : Vec<u64> = vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10];
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.len(), 3);
    }

    #[test]
    fn check_payload_len() {
        let mut writer = MemBufferWriter::new();
        let some_bytes = "Hello how are you?";
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.payload_len(), some_bytes.as_bytes().len()*3);
    }

    #[test]
    fn check_mem_shift() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("Earth");
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result[1..]);
        assert_eq!(reader.is_err(),true);
    }


    #[test]
    fn check_serialize_i32_deserialize() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry(100);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<i32>(0).unwrap(), 100);
    }
}

#[cfg(feature="bench")]
mod bench {
    use test::Bencher;
    use super::{MemBufferWriter,MemBufferReader};
    use serde::{Serialize,Deserialize};
    use serde_json;


    #[bench]
    fn benchmark_few_keys_payload_1mb(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(1_000_000);
        for _ in 0..1_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_entry(&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.load_entry::<&str>(0).unwrap();
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
        writer.add_entry(&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.load_entry::<&str>(0).unwrap();
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
        writer.add_entry(&huge_string);
        let result = writer.finalize();

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string = reader.load_entry::<&str>(0).unwrap();
            assert_eq!(string.len(), 100_000_000);
        });
    }

    #[bench]
    fn benchmark_few_keys_payload_1mb_times_3(b: &mut Bencher) {
        let mut huge_string = String::with_capacity(1_000_000);
        for _ in 0..1_000_000 {
            huge_string.push('a');
        }
        let mut writer = MemBufferWriter::new();
        writer.add_entry(&huge_string);
        writer.add_entry(&huge_string);
        writer.add_entry(&huge_string);
        let result = writer.finalize();
        assert!(result.len() > 3_000_000);

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string1 = reader.load_entry::<&str>(0).unwrap();
            let string2 = reader.load_entry::<&str>(1).unwrap();
            let string3 = reader.load_entry::<&str>(2).unwrap();
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
        writer.add_entry(&huge_string);
        writer.add_entry(&huge_string);
        writer.add_entry(&huge_string);
        let result = writer.finalize();
        assert!(result.len() > 300_000_000);

        b.iter(|| {
            let reader = MemBufferReader::new(&result).unwrap();
            let string1 = reader.load_entry::<&str>(0).unwrap();
            let string2 = reader.load_entry::<&str>(1).unwrap();
            let string3 = reader.load_entry::<&str>(2).unwrap();
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

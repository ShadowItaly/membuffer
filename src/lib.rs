#![cfg_attr(feature = "bench", feature(test))]

#[cfg(feature = "bench")]
extern crate test;


use byteorder::{WriteBytesExt, ReadBytesExt, NativeEndian,ByteOrder};
use serde::{Serialize,Deserialize};
use bincode;
use std::borrow::Cow;


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
///  //The first entry is the key and must be a type that implements Into<i32>
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

///Refers to a position given to every deserialize and serialize operation, can be used to store
///data if one does not need to store data in the payload e. g. Field smaller than 8 Bytes
pub struct Position {
    pub start: i32,
    pub end: i32,
}


///Refers to the different types when implementing your own types use an own enum like
///this:
///```rust
///use membuffer::MemBufferTypes;
///enum MyImplementedTypes {
/// MyOwnType0 = MemBufferTypes::LastPreDefienedValue as isize,
/// MyOwnType1,
/// MyOwnType2
///}
///```
#[derive(Debug)]
pub enum MemBufferTypes {
    Text,
    Integer32,
    VectorU8,
    VectorU32,
    VectorU64,
    MemBuffer,
    LastPreDefienedValue
}

impl Into<i32> for MemBufferTypes {
    fn into(self) -> i32 {
        self as i32
    }
}


struct InternPosition {
    pub pos: Position,
    pub variable_type: i32,
}




#[derive(Debug, Clone)]
pub enum MemBufferError {
    FieldTypeError(i32,i32),
    WrongFormat,
}

impl<'a> std::fmt::Display for MemBufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MemBufferError::FieldTypeError(x,y) => write!(f,"Memory buffer error: Field has type {} and not requested type {}",x.to_string(),y.to_string()),
            MemBufferError::WrongFormat => write!(f,"Memory buffer error: Reached end of slice before end of header, memory seems to be corrupted")
        }
    }
}


pub trait MemBufferDeserialize<'a,T> {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<T,MemBufferError> where Self: Sized;
}

impl<'a> MemBufferDeserialize<'a,&'a str> for &str {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<&'a str,MemBufferError> {
        //This should always be safe as long as the saved string was utf-8 encoded and no one
        //messed with the file on disk.
        unsafe{ Ok(std::str::from_utf8_unchecked(mem)) }
    }
}

impl<'a> MemBufferDeserialize<'a,i32> for i32 {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<i32,MemBufferError> {
        //Fast load integer since no memory is required to store integer
        Ok(NativeEndian::read_i32(mem))
    }
}

impl<'a> MemBufferDeserialize<'a,u64> for u64 {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<u64,MemBufferError> {
        //Fast load integer since no memory is required to store integer
        Ok(NativeEndian::read_u64(mem))
    }
}

impl<'a> MemBufferDeserialize<'a,&'a [u8]> for &[u8] {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<&'a [u8],MemBufferError> {
        Ok(mem)
    }
}

impl<'a> MemBufferDeserialize<'a,&'a [u64]> for &[u64] {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<&'a [u64],MemBufferError> {
        let val: *const u8 = mem.as_ptr();
        let cast_memory = val.cast::<u64>();
        //Divide by eight as u64 should be 8 bytes on any system
        let mem_length = mem.len()>>3;

        //This should always be safe as long as no one messed with the serialized data
        Ok(unsafe{std::slice::from_raw_parts(cast_memory, mem_length as usize)})
    }
}

impl<'a> MemBufferDeserialize<'a,&'a [u32]> for &[u32] {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<&'a [u32],MemBufferError> {
        let val: *const u8 = mem.as_ptr();
        let cast_memory = val.cast::<u32>();
        //Divide by four as u32 should be 4 bytes on any system
        let mem_length = mem.len()>>2;

        //This should always be safe as long as no one messed with the serialized data
        Ok(unsafe{std::slice::from_raw_parts(cast_memory, mem_length as usize)})
    }
}

impl<'a> MemBufferDeserialize<'a,MemBufferReader<'a>> for MemBufferReader<'a> {
    fn from_mem_buffer(mem: &'a [u8]) -> Result<MemBufferReader<'a>,MemBufferError> {
        let reader = MemBufferReader::new(mem)?;
        Ok(reader)
    }
}

///The reader which is used for reading the memory area produced by the writer, **Important notice:
///The reader uses the native endian of the system used therefore sending between big endian and
///little endian systems wont work**
///```rust
///use membuffer::{MemBufferWriter,MemBufferReader};
///
///let mut data = MemBufferWriter::new();
///data.add_entry("Add some data to save to file or send over the network");
///let data_vec = data.finalize();
/////The reader is type sensitive
///let reader = MemBufferReader::new(&data_vec).unwrap();
/////We load the first entry, try not to get this mixed up
///assert_eq!(reader.load_entry::<&str>(0).unwrap(),"Add some data to save to file or send over the network");
///```
pub struct MemBufferReader<'a> {
    offsets: &'a [InternPosition],
    data: &'a [u8]
}

impl<'a> MemBufferReader<'a> {
    ///Deserialize data from a buffer to an i32 integer
    pub fn deserialize_i32_from(mut buffer: &[u8]) -> i32 {
        buffer.read_i32::<NativeEndian>().unwrap()
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn payload_len(&self) -> usize {
        self.data.len()
    }
    
    ///Internal load function this is needed to enable loading nested MemBufferWriters which does
    ///not implement the Deserialize trait
    fn intern_load_entry<X: MemBufferDeserialize<'a,X>>(&self, key: usize, expected_type: i32) -> Result<X,MemBufferError> {
        let entry = &self.offsets[key];
        let is_type = entry.variable_type;
        if is_type != expected_type {
            return Err(MemBufferError::FieldTypeError(is_type,expected_type));
        }
        return X::from_mem_buffer(&self.data[entry.pos.start as usize..entry.pos.end as usize]);
    }

    ///Load one entry with the given type, expecting the serializable trait as well to determine
    ///the integer type, when doing polymorphismus of structures use the same integer for multiple
    ///types
    pub fn load_entry<X: MemBufferDeserialize<'a,X> + MemBufferSerialize>(&self,key: usize) -> Result<X,MemBufferError> {
        self.intern_load_entry(key.into(), X::get_mem_buffer_type())
    }

    ///Loads an entry stored with serde_json and returns it.
    pub fn load_serde_entry<T: Deserialize<'a>>(&self,key: usize) -> Result<T,MemBufferError> {
        let data: &[u8] = self.load_entry(key.into())?;
        Ok(bincode::deserialize(data).unwrap())
    }

    ///Loads a nested MembufferWriter as reader
    pub fn load_recursive_reader(&self, key: usize) -> Result<MemBufferReader<'a>,MemBufferError> {
        self.intern_load_entry(key.into(), MemBufferWriter::get_mem_buffer_type())
    }


    ///Creates a new memory format reader from the given memory slice, as the readed values are
    ///borrowed from the memory slice the reader cannot outlive the memory it borrows from
    pub fn new(val: &'a [u8]) -> Result<MemBufferReader<'a>,MemBufferError> {
        if val.len() < 8 {
            return Err(MemBufferError::WrongFormat);
        }

        let vec_len = MemBufferReader::deserialize_i32_from(val) as usize;
        let checksum = MemBufferReader::deserialize_i32_from(&val[4..]) as usize;
        let start = vec_len*std::mem::size_of::<InternPosition>()+8;
        if val.len() < start || std::num::Wrapping(checksum)+std::num::Wrapping(0x7AFECAFE) != std::num::Wrapping(vec_len) {
            return Err(MemBufferError::WrongFormat);
        }

        unsafe {
        Ok(MemBufferReader {
            offsets: std::slice::from_raw_parts(val[8..].as_ptr().cast::<InternPosition>(),vec_len),
            data: &val[start..]
        })
        }
    }
}

impl<'a> std::fmt::Debug for MemBufferReader<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"Found memory buffer with payload size {}",self.data.len())
    }
}


///The Writer class which sets up the schema and writes it into the memory when finished building
pub struct MemBufferWriter {
    types: Vec<i32>,
    data: Vec<Vec<u8>>
}

pub trait MemBufferSerialize {
    fn to_mem_buffer<'a>(&'a self) -> std::borrow::Cow<'a,[u8]>;
    fn get_mem_buffer_type() -> i32; 
}

impl MemBufferSerialize for &str {
    fn to_mem_buffer<'a>(&'a self) -> std::borrow::Cow<'a,[u8]> {
        std::borrow::Cow::Borrowed(self.as_bytes())
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Text.into()
    }
}

impl MemBufferSerialize for &String {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a,[u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Text.into()
    }
}

impl MemBufferSerialize for i32 {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a, [u8]> {
        Cow::Owned(unsafe{std::mem::transmute::<i32,[u8;4]>(*self)}.to_vec())
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::Integer32.into()
    }
}


impl MemBufferSerialize for u64 {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a, [u8]> {
        Cow::Owned(unsafe{std::mem::transmute::<u64,[u8;8]>(*self)}.to_vec())
    }

    fn get_mem_buffer_type() -> i32 {
        1021
    }
}

impl MemBufferSerialize for &[u8] {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a, [u8]> {
        Cow::Borrowed(self)
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::VectorU8.into()
    }
}

impl MemBufferSerialize for &[u64] {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a,[u8]> {
        let val: *const u64 = self.as_ptr();
        let cast_memory = val.cast::<u8>();
        let mem_length = self.len() * std::mem::size_of::<u64>();
        Cow::Borrowed(unsafe{ std::slice::from_raw_parts(cast_memory, mem_length)})
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::VectorU64.into()
    }
}

impl MemBufferSerialize for &[u32] {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a,[u8]> {
        let val: *const u32 = self.as_ptr();
        let cast_memory = val.cast::<u8>();
        let mem_length = self.len() * std::mem::size_of::<u32>();
        Cow::Borrowed(unsafe{ std::slice::from_raw_parts(cast_memory, mem_length)})
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::VectorU32.into()
    }
}


impl MemBufferSerialize for MemBufferWriter {
    fn to_mem_buffer<'a>(&'a self) -> Cow<'a,[u8]> {
        let ret = self.finalize();
        Cow::Owned(ret)
    }

    fn get_mem_buffer_type() -> i32 {
        MemBufferTypes::MemBuffer.into()
    }
}

impl MemBufferWriter {
    ///Creates a new empty memory format writer
    pub fn new() -> MemBufferWriter {
        MemBufferWriter {
            types: Vec::new(),
            data: Vec::new()
        }
    }

    ///Create a new Membuffer writer from the given memory, this will enable the writer to add
    ///more data to the previous version, to do so the writer does a full reload of the memory
    ///therefore it is an expensive operation if the structure adding fields to is huge.
    ///```rust
    ///use membuffer::{MemBufferWriter,MemBufferReader};
    ///
    ///let mut value = MemBufferWriter::new();
    ///value.add_entry("Hello");
    ///value.add_entry("World");
    ///
    ///let data = value.finalize();
    ///
    /////Save data to disk or anything like that
    /////Then load it again and add more data by doing this
    ///
    ///let mut writer_adder = MemBufferWriter::from(&data).unwrap();
    /////The writer creates a new Vector to hold the data therefore no mutable reference to data is
    /////stored
    ///writer_adder.add_entry("Damn I forgot");
    ///
    ///let new_data = writer_adder.finalize();
    /////new_data will now contain an entry for "Hello" an entry for "World" and an entry
    /////for "Damn I forgot" 
    ///
    ///```
    pub fn from<'a>(raw_memory: &'a [u8]) -> Result<MemBufferWriter,MemBufferError> {
        let reader = MemBufferReader::new(raw_memory)?;
        let mut types : Vec<i32> = Vec::new();
        let mut data : Vec<Vec<u8>> = Vec::new();
        for x in reader.offsets.iter() {
            types.push(x.variable_type);
            data.push(reader.data[x.pos.start as usize..x.pos.end as usize].to_vec())
        }

        Ok(MemBufferWriter {
            types,
            data
        })
    }

    ///Serializes the integer to the memory slice
    pub fn serialize_i32_to(val: i32, to: &mut Vec<u8>) {
        to.write_i32::<NativeEndian>(val).unwrap();
    }

    ///Adds an entry to the writer the only requirement is the serializable trait
    pub fn add_entry<T: MemBufferSerialize>(&mut self, val: T) {
        let slice = val.to_mem_buffer();
        self.types.push(T::get_mem_buffer_type());
        self.data.push(slice.to_vec());
    }

    pub fn set_entry<T: MemBufferSerialize>(&mut self, val: T, index: usize) {
        self.data[index] = val.to_mem_buffer().to_vec();
        self.types[index] = T::get_mem_buffer_type();
    }

    pub fn load_entry<'a, T: MemBufferDeserialize<'a,T>+MemBufferSerialize>(&'a mut self, index: usize) -> Result<T,MemBufferError> {
        if T::get_mem_buffer_type() != self.types[index] {
            return Err(MemBufferError::FieldTypeError(self.types[index],T::get_mem_buffer_type()));
        }
        return T::from_mem_buffer(&self.data[index]);
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    ///Adds a serde serializable entry into the structure as serializer serde_json is used.
    ///Internally it is saved as a string.
    pub fn add_serde_entry<T: Serialize>(&mut self,val: &T) {
        let as_str = bincode::serialize(val).unwrap();
        self.add_entry(&as_str[..]);
    }


    ///Finalize the schema and return the memory slice holding the whole vector
    pub fn finalize(&self) -> Vec<u8> {
        let mut var: Vec<u8> = Vec::with_capacity(10_000_000);
        MemBufferWriter::serialize_i32_to(self.types.len() as i32,&mut var);
        MemBufferWriter::serialize_i32_to((std::num::Wrapping(self.types.len() as i32)-std::num::Wrapping(0x7AFECAFE as i32)).0,&mut var);
        let mut offset = 0;
        for val in 0..self.types.len() {
            MemBufferWriter::serialize_i32_to(offset as i32, &mut var);
            MemBufferWriter::serialize_i32_to(self.data[val].len() as i32+offset as i32, &mut var);
            MemBufferWriter::serialize_i32_to(self.types[val], &mut var);
            offset+=self.data[val].len();
        }
        for x in self.data.iter() {
            var.extend_from_slice(x);
        }
        var
    }
}



#[cfg(test)]
mod tests {
    use super::{MemBufferWriter,MemBufferReader,MemBufferError,MemBufferTypes,MemBufferSerialize};
    use serde::{Serialize,Deserialize};

    #[derive(Serialize,Deserialize)]
    struct HeavyStruct {
        vec: Vec<u64>,
        name: String,
        frequency: i32,
        id: i32,
    }

    #[test]
    fn check_enum_usage() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("Der moderne Prometheus");
        writer.add_entry("Dies hier ist nur ein Satz");
        writer.add_entry::<&[u64]>(&vec![0,1,2,3,4,5]);

        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();

        let _: &str = reader.load_entry(0).unwrap();
        let _: &str = reader.load_entry(1).unwrap();
        let _: &[u64] = reader.load_entry(2).unwrap();
    }

    #[test]
    fn check_vec32() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry::<&[u32]>(&vec![0,1,2,3,4,5]);

        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();

        let val: &[u32] = reader.load_entry(0).unwrap();
        assert_eq!(vec![0,1,2,3,4,5],val);
    }
    
    #[test]
    fn check_type_ids() {
        assert_eq!(<&str as MemBufferSerialize>::get_mem_buffer_type(),MemBufferTypes::Text as i32);
        assert_eq!(<&String as MemBufferSerialize>::get_mem_buffer_type(),MemBufferTypes::Text as i32);
        assert_eq!(<i32 as MemBufferSerialize>::get_mem_buffer_type(),MemBufferTypes::Integer32 as i32);
        assert_eq!(<&[u8] as MemBufferSerialize>::get_mem_buffer_type(),MemBufferTypes::VectorU8 as i32);
        assert_eq!(<&[u64] as MemBufferSerialize>::get_mem_buffer_type(),MemBufferTypes::VectorU64 as i32);
        assert_eq!(MemBufferWriter::get_mem_buffer_type(),MemBufferTypes::MemBuffer as i32);
    }

    #[test]
    fn corrupt_length() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("Der moderne Prometheus");
        writer.add_entry("Dies hier ist nur ein Satz");
        writer.add_entry::<&[u64]>(&vec![0,1,2,3,4,5]);

        let mut result = writer.finalize();
        result[0] = 100;


        let reader = MemBufferReader::new(&result);
        assert_eq!(reader.is_err(),true);
    }

    #[test]
    fn check_read_attributes() {
        let mut writer = MemBufferWriter::new();
        let str1 = "Hello World";
        let str2 = "Hello second World";
        let str3 = "визитной карточкой";
        writer.add_entry(str1);
        writer.add_entry(str2);
        writer.add_entry(str3);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        let positions = &reader.offsets;

        assert_eq!(positions.len(),3);
        let zero = &positions[0];
        assert_eq!(zero.variable_type,MemBufferTypes::Text as i32);
        assert_eq!(zero.pos.start,0);
        assert_eq!(zero.pos.end - zero.pos.start,str1.as_bytes().len() as i32);

        let one = &positions[1];
        assert_eq!(one.variable_type,MemBufferTypes::Text as i32);
        assert_eq!(one.pos.start,str1.as_bytes().len() as i32);
        assert_eq!(one.pos.end - one.pos.start,str2.as_bytes().len() as i32);

        let two = &positions[2];
        assert_eq!(two.variable_type,MemBufferTypes::Text as i32);
        assert_eq!(two.pos.start as usize,str1.as_bytes().len() + str2.as_bytes().len());
        assert_eq!(two.pos.end - two.pos.start,str3.as_bytes().len() as i32);

        assert_eq!(reader.load_entry::<&str>(2).unwrap(),str3);
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
        let string = String::from("ok nice");
        writer.add_entry("Earth");
        writer.add_entry::<&String>(&string);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&str>(0).unwrap(), "Earth");
        assert_eq!(reader.load_entry::<&str>(1).unwrap(), "ok nice");
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

    #[should_panic]
    #[test]
    fn check_wrong_key() {
        let mut writer = MemBufferWriter::new();
        let some_bytes : Vec<u64> = vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10];
        writer.add_entry(&some_bytes[..]);
        writer.add_entry(&some_bytes[..]);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&[u64]>(0).unwrap(), vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10]);
        //TODO check index overflow
        assert_eq!(reader.load_entry::<&[u64]>(3).unwrap(), vec![100,200,100,200,1,2,3,4,5,6,7,8,9,10]);
    }

    #[test]
    fn check_reload_writer_from_mem() {
        let mut writer = MemBufferWriter::new();
        let str1 = "Hello World";
        let str2 = "Hello second World";
        let str3 = "визитной карточкой";
        writer.add_entry(str1);
        writer.add_entry(str2);
        writer.add_entry(str3);
        let result = writer.finalize();

        let mut writer2 = MemBufferWriter::from(&result).unwrap();
        writer2.add_entry("fuchs");
        
        let added2 = writer2.finalize();
        let reader = MemBufferReader::new(&added2).unwrap();
        assert_eq!(reader.len(),4);
        assert_eq!(reader.load_entry::<&str>(3).unwrap(),"fuchs");
        assert_eq!(reader.load_entry::<&str>(2).unwrap(),"визитной карточкой");
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
    fn check_empty() {
        let writer = MemBufferWriter::new();
        let result = writer.finalize();
        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.len(), 0);
        assert_eq!("Found memory buffer with payload size 0",format!("{:?}",reader));

    }

    #[test]
    fn check_slice_too_small() {
        let writer = MemBufferWriter::new();
        let result = writer.finalize();
        let reader = MemBufferReader::new(&result[0..1]);
        assert_eq!(reader.is_err(),true);
        println!("Error: {}",reader.unwrap_err());
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
    fn check_recursive_readers() {
        let mut writer = MemBufferWriter::new();
        let some_bytes = "Hello how are you?";
        writer.add_entry(&some_bytes[..]);

        let mut writer2 = MemBufferWriter::new();
        writer2.add_entry(some_bytes);

        writer.add_entry(writer2);
        let result = writer.finalize();
        assert_eq!(writer.finalize(), result);

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.len(), 2);
        assert_eq!(reader.load_entry::<&str>(0).unwrap(), "Hello how are you?");
        let second = reader.load_recursive_reader(1);
        assert_eq!(second.is_err(),false);
        let reader2 = second.unwrap();
        assert_eq!(reader2.len(), 1);
        assert_eq!(reader2.load_entry::<&str>(0).unwrap(), "Hello how are you?");

        assert_eq!(reader.load_recursive_reader(0).is_err(),true);
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
    fn check_mem_set_entry() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("earth");
        writer.set_entry("cool", 0);
        let result = writer.finalize();

        let reader = MemBufferReader::new(&result).unwrap();
        assert_eq!(reader.load_entry::<&str>(0).unwrap(),"cool");
    }

    #[test]
    fn check_type_error() {
        let mut writer = MemBufferWriter::new();
        writer.add_entry("Earth");

        assert_eq!(writer.load_entry::<&str>(0).unwrap(),"Earth");
        let error = writer.load_entry::<i32>(0).unwrap_err();
        if let MemBufferError::FieldTypeError(x,y) = error {
                assert_eq!(x, MemBufferTypes::Text as i32);
                assert_eq!(y, MemBufferTypes::Integer32 as i32);
        }
 

        let result = writer.finalize();

        let reader = MemBufferReader::new(&result);
        assert_eq!(reader.is_err(),false);
        let err = reader.unwrap().load_entry::<i32>(0).unwrap_err();
        if let MemBufferError::FieldTypeError(x,y) = err {
                println!("Error {} ",MemBufferError::FieldTypeError(x,y));
                assert_eq!(x, MemBufferTypes::Text as i32);
                assert_eq!(y, MemBufferTypes::Integer32 as i32);
        }
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
    use bincode;


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

        let string = bincode::serialize(&first).unwrap();

        b.iter(|| {
            let reader: BenchSerde = bincode::deserialize(&string).unwrap();
            assert_eq!(reader.one.len(), 1_000_000);
            assert_eq!(reader.two.len(), 1_000_000);
            assert_eq!(reader.three.len(), 1_000_000);
        });
    }
}

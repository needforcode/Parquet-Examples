package org.way2bigdata.parquet;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

public class ParquetAvroConvJava {

  public static ParquetWriter parquetWriter;
  
  public static void main(String args[]) throws Exception {
    
    if (args.length != 2) {
      throw new Exception("Pass input and output path");
    }
    String inputFile = args[0];
    String outputFile = args[1];
    
    ClassLoader classLoader = ParquetAvroConvJava.class.getClassLoader();
    InputStream in = classLoader.getResourceAsStream("twitter.avsc");
    Schema avroSchema = new Schema.Parser().parse(in);
    System.out.println("avroschema: "+avroSchema);
	// generate the corresponding Parquet schema
	MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
	
	System.out.println("parquetschema: " +parquetSchema);
	 
	// create a WriteSupport object to serialize your Avro objects
	AvroWriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
	 
	// choose compression scheme
	CompressionCodecName compressionCodec = CompressionCodecName.UNCOMPRESSED;
	
	// set Parquet file block size and page size values
	int blockSize = 256 * 1024 * 1024;
	int pageSize = 64 * 1024;
	 
	Path outputPath = new Path(outputFile);
     
	File f=new File(outputFile);
     if(f.exists()){
         f.delete();
     }
	
	GenericRecord emp = new GenericData.Record(avroSchema);
    
	DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(inputFile), datumReader);
	 
	// the ParquetWriter object that will consume Avro GenericRecords
	parquetWriter = new ParquetWriter(outputPath, writeSupport);
	
	GenericRecord record = null;
	 while(dataFileReader.iterator().hasNext()) {
	   record = dataFileReader.next(record);
	   System.out.println("record: "+record);
	    parquetWriter.write(record);
	}
	 
	 parquetWriter.close();
}
}

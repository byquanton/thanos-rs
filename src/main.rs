mod utils;

use crate::utils::copy_except_region;
use clap::{Arg, Command};
use flate2::read::GzDecoder;
use flate2::read::ZlibDecoder;
use rayon::prelude::*;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

const SECTOR_SIZE: u64 = 4096;

fn main() {
    let cmd = Command::new("thanos-rs")
        .arg(Arg::new("input_dir")
            .required(true)
            .help("Path to the world directory"))
        .arg(Arg::new("output_dir")
            .required(false)
            .help("Path to the output directory"))
        .arg(Arg::new("inhabited-time")
            .default_value("0")
            .required(false)
            .short('i')
            .help("Specify the maximum Inhabited Time threshold, measured in ticks, for deleting chunks"))
        .arg(Arg::new("threads")
            .default_value("4")
            .required(false)
            .short('t')
            .help("Specify the number of threads to use for processing"));
    let matches = cmd.get_matches();

    let input_dir = matches.get_one::<String>("input_dir").unwrap();
    let output_dir = matches.get_one::<String>("output_dir").unwrap_or(input_dir);
    let inhabited_time_threshold: i64 = matches.get_one::<String>("inhabited-time").unwrap().parse().unwrap();
    let num_threads: usize = matches.get_one::<String>("threads").unwrap().parse().unwrap();

    rayon::ThreadPoolBuilder::new().num_threads(num_threads).build_global().unwrap();

    if !Path::new(input_dir).exists() {
        eprintln!("error: input directory does not exist.");
        std::process::exit(1);
    }

    if input_dir != output_dir {
        if Path::new(output_dir).exists() {
            eprintln!("error: output directory already exists.");
            std::process::exit(1);
        }

        std::fs::create_dir(output_dir).unwrap_or_else(|err| {
            eprintln!("error: couldn't create output directory - {}", err);
            std::process::exit(1);
        });

        println!("Copying world files from '{}' to '{}'.", input_dir, output_dir);
        copy_except_region(input_dir, output_dir).unwrap_or_else(|err| {
            eprintln!("error: failed to copy files - {}", err);
            std::process::exit(1);
        });
        println!("Copied world files");

        std::fs::create_dir(format!("{}/region", output_dir)).unwrap_or_else(|err| {
            eprintln!("error: couldn't create region directory - {}", err);
            std::process::exit(1);
        });
    }

    // TODO: Detect other Dimensions
    optimise_region_files(format!("{input_dir}/region").as_str(), format!("{output_dir}/region").as_str(), inhabited_time_threshold).expect("TODO: panic message");
}


fn optimise_region_files(input_directory: &str, output_directory: &str, inhabited_time_threshold: i64) -> std::io::Result<()> {
    let input_directory = Path::new(input_directory);
    let output_directory = Path::new(output_directory);

    let equal_input_output = input_directory.canonicalize()? == output_directory.canonicalize()?;

    let input_files: Vec<_> = std::fs::read_dir(input_directory)?
        .filter_map(Result::ok)
        .collect();

    input_files.par_iter().for_each(|file_entry| {
        let file_path = file_entry.path();
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        // Filename of a Region File: r.[region_x].[region_z].mca
        if !file_name.ends_with(".mca") {
            return;
        }

        let parts: Vec<&str> = file_name.split('.').collect();
        if parts.len() != 4 {
            return;
        }

        let region_x = parts[1].parse::<i32>().unwrap();
        let region_z = parts[2].parse::<i32>().unwrap();

        let mut file = std::fs::File::open(file_path.clone()).unwrap();
        let file_len = file.metadata().unwrap().len();

        // Skipping/Removing empty region files
        if file_len == 0 {
            if equal_input_output {
                println!("Removing empty region file {}", file_name);
                std::fs::remove_file(file_entry.path()).unwrap();
            } else {
                println!("Skipping empty region file {}", file_name);
            }
            return;
        }

        // Each region file begins with two 4KiB tables, the first containing the chunk locations and the second containing the last modified timestamps of the chunks.
        // If the file is smaller than the two tables, the headers are missing
        if file_len < 2 * SECTOR_SIZE {
            println!("Missing headers in region file!");
            return;
        }

        let mut location_table = vec![0; SECTOR_SIZE as usize];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut location_table).unwrap();

        let mut chunk_data = Vec::new();

        // A region is made up of chunks in a 32 by 32 area.
        for x in 0..32 {
            for z in 0..32 {
                let index = (x & 31) + (z & 31) * 32; // Calculates the index of the Chunk in the location_table
                let loc = i32::from_be_bytes([location_table[index * 4], location_table[index * 4 + 1], location_table[index * 4 + 2], location_table[index * 4 + 3]]);

                let num_sectors = loc & 0xFF;
                let sector_offset = loc >> 8;

                if sector_offset == 0 && num_sectors == 0 {
                    continue; // Skips not generated chunks
                }

                // Seek to the position where the chunk header is located
                // The size of the header is 5 bytes and followed by the stored chunk data
                file.seek(SeekFrom::Start((sector_offset as u64) * SECTOR_SIZE)).unwrap();

                let mut chunk_size = [0; 4]; // Chunk data size is specified in the first 4 bytes
                file.read_exact(&mut chunk_size).unwrap();
                let chunk_size = i32::from_be_bytes(chunk_size);

                let mut compression_type = [0; 1]; // Compression type is specified in the last byte
                file.read_exact(&mut compression_type).unwrap();
                let compression_type = compression_type[0];

                // Reads the chunk data with the calculated chunk size
                let mut data = vec![0; chunk_size as usize - 1];
                file.read_exact(&mut data).unwrap();

                // TODO: Unused, maybe useful for Debugging Outputs?
                let _chunk_x = region_x * 32 + x as i32;
                let _chunk_z = region_z * 32 + z as i32;

                if compression_type != 1 && compression_type != 2 {
                    eprintln!("Error: unknown chunk data compression method: {}!", compression_type);
                    continue;
                }

                let mut decompressed_chunk_data: Vec<u8> = Vec::new();
                match compression_type {
                    1 => {
                        let mut gz = GzDecoder::new(Cursor::new(data.clone()));
                        if let Err(err) = gz.read_to_end(&mut decompressed_chunk_data) {
                            eprintln!("Error decompressing chunk data: {}", err);
                            continue;
                        }
                    }
                    2 => {
                        let mut zlib = ZlibDecoder::new(Cursor::new(data.clone()));
                        if let Err(err) = zlib.read_to_end(&mut decompressed_chunk_data) {
                            eprintln!("Error decompressing chunk data: {}", err);
                            continue;
                        }
                    }
                    _ => unreachable!(),
                }

                let nbt = simdnbt::borrow::read(&mut Cursor::new(&*decompressed_chunk_data)).expect("Failed to read chunk data").unwrap();
                let inhabited_time = nbt.long("InhabitedTime").unwrap();

                if inhabited_time > inhabited_time_threshold {
                    chunk_data.push((loc, compression_type, data));
                }
            }
        }

        if chunk_data.is_empty() {
            // TODO: Make this message only shop up when debug output is active (implement logging?)
            // println!("Skipping region file {} as it has no chunks left after optimisation", file_name);
            return;
        }

        // TODO: Clean up the following code and add comments
        let mut output_file = std::fs::File::create(format!("{}/{}", output_directory.display(), file_name)).unwrap();
        let mut offset = 2 * SECTOR_SIZE;
        for (loc, compression_type, data) in chunk_data {
            let num_sectors = (data.len() as u64 + SECTOR_SIZE - 1) / SECTOR_SIZE;
            let new_loc = (offset / SECTOR_SIZE) << 8 | num_sectors;
            output_file.seek(SeekFrom::Start((loc & 0xFF) as u64 * 4)).unwrap();
            output_file.write_all(&new_loc.to_be_bytes()).unwrap();

            output_file.seek(SeekFrom::Start(offset)).unwrap();
            output_file.write_all(&(data.len() as i32 + 1).to_be_bytes()).unwrap();
            output_file.write_all(&[compression_type]).unwrap();
            output_file.write_all(&data).unwrap();

            offset += num_sectors * SECTOR_SIZE;
        }

        output_file.set_len(offset).unwrap();
    });

    Ok(())
}


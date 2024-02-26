extern crate image;

use image::{ImageBuffer, Rgb};
use dbase::FieldValue;
use tiff::decoder::Decoder;
use tiff::decoder::Limits;
use std::fs::File;
use std::io::BufReader;
use tqdm::tqdm;
use std::collections::HashMap;
use rayon::prelude::*;
use std::sync::{Arc, RwLock};
use std::env;


struct PixelMapping {
    value: u16,
    terrain: u16,
    vegetation : u16,
    temperature: u16,
    moisture: u16,
}

fn read_database_mappings(dbf_path: &str) -> Vec<PixelMapping> {
    let mut mappings = Vec::new();
    let records = dbase::read(dbf_path).unwrap();
    for record in records {
        let mut pixel = PixelMapping { value: 0, terrain: 0, vegetation: 0, temperature: 0, moisture: 0 };
        for (name, value) in record {
            match value {
                FieldValue::Numeric(n) => {
                    match name.as_str() {
                        "Value" => pixel.value = n.unwrap() as u16,
                        "World_Lan1" => pixel.terrain = n.unwrap() as u16,
                        "World_Lan2" => pixel.vegetation = n.unwrap() as u16,
                        "World_Temp" => pixel.temperature = n.unwrap() as u16,
                        "World_Mois" => pixel.moisture = n.unwrap() as u16,
                        _ => {}
                    }
                },
                _ => {}
            }
        }
        mappings.push(pixel);
    }
    return mappings;
}



fn decode_image(file_path: &str, database_path: &str, scale: u32) -> Vec<ImageBuffer<Rgb<u8>, Vec<u8>>> {
    println!("Loading image...");
    let file = File::open(file_path).unwrap();
    let mut decoder = Decoder::new(BufReader::new(file)).expect("decoder failed").with_limits(Limits::unlimited());

    let src_pixels = match decoder.read_image().unwrap() {
        tiff::decoder::DecodingResult::U16(src_pixels) => src_pixels,
        _ => panic!("Unsupported image format")
    };
    let (width, height) = decoder.dimensions().unwrap();


    let images = set_pixels(src_pixels, database_path, scale, width, height);

    return images;
}

fn set_pixels(src_pixels: Vec<u16>, database_path: &str, scale: u32, width: u32, height: u32) -> Vec<ImageBuffer<Rgb<u8>, Vec<u8>>> {

    let mut image_terrain = ImageBuffer::new(width / scale, height / scale);
    let image_vegetation = Arc::new(RwLock::new(ImageBuffer::new(width / scale, height / scale)));
    let image_temperature = Arc::new(RwLock::new(ImageBuffer::new(width / scale, height / scale)));
    let image_moisture = Arc::new(RwLock::new(ImageBuffer::new(width / scale, height / scale)));

    // Clone Arc references for each image
    let image_vegetation_clone = Arc::clone(&image_vegetation);
    let image_temperature_clone = Arc::clone(&image_temperature);
    let image_moisture_clone = Arc::clone(&image_moisture);

    let max_db_value = src_pixels.iter().max().unwrap();
    let mappings = read_database_mappings(database_path);
    tqdm(image_terrain.enumerate_pixels_mut()).par_bridge().for_each(|(x, y, pixel)| {
        let mut terrain = Vec::new();
        let mut vegetation  = Vec::new();
        let mut temperature = Vec::new();
        let mut moisture = Vec::new();
        for i in 0..scale {
            for j in 0..scale {
                let index = ((y as u64 * scale as u64 + j as u64) * width as u64 + (x as u64 * scale as u64 + i as u64)) as usize;
                let pixel_value = src_pixels[index];
                if pixel_value < *max_db_value {
                    let mapping = map_pixel(pixel_value, &mappings);
                    terrain.push(mapping.terrain);
                    vegetation.push(mapping.vegetation);
                    temperature.push(mapping.temperature);
                    moisture.push(mapping.moisture);
                } else {
                    terrain.push(0);
                    vegetation.push(0);
                    temperature.push(0);
                    moisture.push(0);
                }
            }
        }
        let color = match most_common(&terrain){
            1 => Rgb([128, 128, 128]), // Mountains (gray)
            2 => Rgb([139, 69, 19]),   // Hills (brown)
            3 => Rgb([232, 193, 148]), // Tablelands (light brown)
            4 => Rgb([98, 188, 47]),   // Plains (green)
            _ => Rgb([35,137,218]),     // Water (blue)
        };
        *pixel = color;

        let color = match most_common(&vegetation) {
            1 => Rgb([0, 128, 0]),    // Cropland (green)
            2 => Rgb([139, 69, 19]),  // Shrubland (brown)
            3 => Rgb([0, 128, 0]),    // Forest (green)
            4 => Rgb([0, 255, 0]),    // Grassland (bright green)
            5 => Rgb([255, 0, 0]),    // Settlement (red)
            6 => Rgb([128, 128, 128]), // Sparsely or Non-vegetated (gray)
            8 => Rgb([255, 255, 255]), // Snow and Ice (white)
            _ => Rgb([35,137,218]),      // Not Land (black)
        };
        image_vegetation_clone.write().unwrap().put_pixel(x, y, color);

        let color = match most_common(&temperature) {
            1 => Rgb([0, 0, 255]),    // Boreal (blue)
            2 => Rgb([0, 128, 255]),  // Cool Temperate (light blue)
            3 => Rgb([0, 255, 255]),  // Warm Temperate (cyan)
            4 => Rgb([255, 255, 0]),  // Sub Tropical (yellow)
            5 => Rgb([255, 0, 0]),    // Tropical (red)
            6 => Rgb([255, 255, 255]), // Polar (white)
            _ => Rgb([35,137,218]),      // Not Land (black)
        };
        image_temperature_clone.write().unwrap().put_pixel(x, y, color);

        let color = match most_common(&moisture) {
            1 => Rgb([255, 255, 0]), // Desert (yellow)
            2 => Rgb([255, 128, 0]), // Dry (orange)
            3 => Rgb([0, 255, 0]),   // Moist (green)
            _ => Rgb([35,137,218]),     // Not Land (black)
        };
        image_moisture_clone.write().unwrap().put_pixel(x, y, color);
    });
    return vec![
        image_terrain,
        image_vegetation.read().unwrap().clone(),
        image_temperature.read().unwrap().clone(),
        image_moisture.read().unwrap().clone(),
    ];
}

fn map_pixel(pixel_value: u16, mappings: &[PixelMapping]) -> &PixelMapping {
    for mapping in mappings {
        if mapping.value == pixel_value {
            return mapping;
        }
    }
    panic!("No mapping found for pixel value {}", pixel_value);
}

fn most_common(terrain: &[u16]) -> u16 {
    let mut counts = HashMap::new();
    for &t in terrain {
        *counts.entry(t).or_insert(0) += 1;
    }
    let mut max_count = 0;
    let mut most_common = 0;
    for (t, count) in counts {
        if count > max_count {
            max_count = count;
            most_common = t;
        }
    }
    most_common
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("Usage: {} <input_file> <output_folder>", args[0]);
        return;
    }
    let input_file = &args[1];
    let output_folder = &args[2];
    let scale = 4;
    let imgs = decode_image(input_file, "world.dbf", scale);
    
    println!("Saving images...");
    imgs[0].save(format!("{}/terrain.png", output_folder)).unwrap();
    imgs[1].save(format!("{}/vegetation.png", output_folder)).unwrap();
    imgs[2].save(format!("{}/temperature.png", output_folder)).unwrap();
    imgs[3].save(format!("{}/moisture.png", output_folder)).unwrap();
    println!("Done!");
}


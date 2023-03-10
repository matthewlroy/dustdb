use rand::Rng;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::num::ParseIntError;
use dustcfg::get_env_var;

/// DustDB v0.1.0
/// Matthew Roy <matthew@saplink.io>
///
/// Operations supported:
///
/// 1. [C]reate in storage.
/// 2. [R]ead from storage.
/// 3. [U]pdate data already in storage.
/// 4. [D]elete from storage.

fn main() -> Result<(), io::Error> {
    // https://doc.rust-lang.org/std/io/fn.stdout.html
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let response;

    let args: Vec<_> = env::args().collect();

    if args.len() > 2 {
        match args[1].as_str() {
            "create" => {
                // TODO: Error check that args[2] is valid string
                // TODO: Error check that args[3] is valid JSON string
                response = create(args[2].as_str(), args[3].as_str())
            }
            "read" => {
                // TODO: Error check that args[2] is valid v4 uuid
                response = read()
            }
            "update" => {
                // TODO: Error check that args[2] is valid v4 uuid
                // TODO: Error check that args[3] is valid JSON string
                response = update()
            }
            "delete" => {
                // TODO: Error check that args[2] is valid v4 uuid
                response = delete()
            }
            _ => response = print_command_help(),
        }
    } else {
        response = print_command_help();
    }

    // TODO: Handle the error with the nomenclature of ERR,<ERR_MSG> from main
    handle.write_all(response.as_bytes())?;
    Ok(())
}

fn create(pile_name: &str, seralized_json_as_hex: &str) -> String {
    // STEP 1: Generate a UUID to be used for future ops
    let generated_uuid: String = generate_v4_uuid();

    // STEP 2: Create the path for the desired pile (if not exists)
    let file_path = format!("{}/{}", get_env_var("DUST_DATA_DIR"), pile_name);
    let create_pile_result = fs::create_dir_all(&file_path);
    match create_pile_result {
        Ok(()) => {
            // STEP 3: Write the data supplied to the file at the determined path
            // NOTE: We are writing the DECODED HEX to the file! This makes it
            // easier for future viewing.
            let new_file_result = fs::write(
                format!(
                    "{}/{}.{}",
                    file_path,
                    generated_uuid,
                    get_env_var("DUST_DATA_FMT")
                ),
                decode_hex_to_ascii(seralized_json_as_hex),
            );

            // TODO: Check for uuid collision ?

            match new_file_result {
                Ok(()) => "OK,".to_owned() + &generated_uuid,
                Err(e) => "ERR,".to_owned() + &e.to_string(),
            }
        }
        Err(e) => "ERR,".to_owned() + &e.to_string(),
    }
}

fn decode_hex_to_ascii(text_to_decode: &str) -> String {
    let v: Result<Vec<u8>, ParseIntError> = (0..text_to_decode.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text_to_decode[i..i + 2], 16))
        .collect(); // TODO: Error checking

    let v_as_bytes: Vec<u8> = v.unwrap();
    let s: String = String::from_utf8_lossy(&v_as_bytes).to_string();
    s
}

fn read() -> String {
    let empty: String = "read".to_owned();
    empty
}

fn update() -> String {
    let empty: String = "update".to_owned();
    empty
}

fn delete() -> String {
    let empty: String = "delete".to_owned();
    empty
}

/// The available commands, returned as a String on errors, for DustDB.
fn print_command_help() -> String {
    let mut command_help = String::new();

    command_help.push_str("ERR,Invalid input.");
    command_help.push_str(" Command line arguments available:");
    command_help.push_str(" {create <SERIALIZED_JSON_AS_BINARY>}");
    command_help.push_str(" {read <UUID>}");
    command_help.push_str(" {update <UUID> <SERIALIZED_JSON_AS_BINARY>}");
    command_help.push_str(" {delete <UUID>}");

    command_help
}

/// The procedure to generate a version 4 UUID is as follows:
///
/// >> In RFC Technical Terms:
/// >> https://www.rfc-editor.org/rfc/rfc4122#page-14
///
/// 4.4. Algorithms for Creating a UUID from Truly Random or Pseudo-Random
/// Numbers. The version 4 UUID is meant for generating UUIDs from truly-random
/// or pseudo-random numbers. The algorithm is as follows:
///
/// o Set the two most significant bits (bits 6 and 7) of the
///     clock_seq_hi_and_reserved to zero and one, respectively.
/// o Set the four most significant bits (bits 12 through 15) of the
///     time_hi_and_version field to the 4-bit version number from Section 4.1.3
/// o Set all the other bits to randomly (or pseudo-randomly) chosen values.
///
/// >> In Plain Language Terms:
/// >> https://www.cryptosys.net/pki/uuid-rfc4122.html
///
/// 1. Generate 16 random bytes (=128 bits)
/// 2. Adjust certain bits according to RFC 4122 section 4.4 as follows:
///     a. set the four most significant bits of the 7th byte to 0100'B, so the
///         high nibble is "4"
///     b. set the two most significant bits of the 9th byte to 10'B, so the
///         high nibble will be one of "8", "9", "A", or "B" (see Note 1).
/// 3. Encode the adjusted bytes as 32 hexadecimal digits
/// 4. Add four hyphen "-" characters to obtain blocks of 8, 4, 4, 4 and 12 hex
///     digits
/// 5. Output the resulting 36-character string
///     "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
fn generate_v4_uuid() -> String {
    let mut uuid_v4 = String::new();

    // 1. Generate 16 random bytes (=128 bits)
    let mut rng = rand::thread_rng();
    for x in 0..16 {
        let mut n = rng.gen::<u8>();

        // 2a. set the four most significant bits of the 7th byte to 0100'B, so
        // the high nibble is "4"
        if x == 6 {
            let first_and = 0b00001111u8;
            let second_or = 0b01000000u8;
            n = (n & first_and) | second_or;
        }

        // 2b. set the two most significant bits of the 9th byte to 10'B, so the
        // high nibble will be one of "8", "9", "A", or "B" (see Note 1).
        if x == 8 {
            let first_and = 0b00111111u8;
            let second_or = 0b10000000u8;
            n = (n & first_and) | second_or;
        }

        // 4. Add four hyphen "-" characters to obtain blocks of 8, 4, 4, 4 and
        // 12 hex digits
        if uuid_v4.len() == 8 {
            uuid_v4.push('-');
        }

        if uuid_v4.len() == 8 + 4 + 1 {
            uuid_v4.push('-');
        }

        if uuid_v4.len() == 8 + 4 + 4 + 2 {
            uuid_v4.push('-');
        }

        if uuid_v4.len() == 8 + 4 + 4 + 4 + 3 {
            uuid_v4.push('-');
        }

        uuid_v4.push_str(&format!("{:02x}", n));

        // println!(
        //     "Index [{}]:\t{:#010b}\t(Byte #{})\t=>\t{}\t=>\t{:02x}",
        //     x,
        //     n,
        //     x + 1,
        //     n,
        //     n
        // );
    }

    // 5. Output the resulting 36-character string
    // "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    uuid_v4
}

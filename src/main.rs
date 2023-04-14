/// DustDB v0.1.0
/// Matthew Roy <matthew@saplink.io>
///
/// Operations supported:
///
/// 1. [C]reate in storage.
/// 2. [R]ead from storage.
/// 3. [U]pdate data already in storage.
/// 4. [D]elete from storage.
use chrono::Utc;
use dustcfg::{decode_hex_to_utf8, encode_utf8_to_hex, generate_v4_uuid, get_env_var};
use dustlog::{write_to_log, DBRequestLog, DBResponseLog, LogLevel};
use futures::SinkExt;
use serde_json::{from_str, Value};
use std::fs;
use std::mem::size_of_val;
use std::path::Path;
use std::{error::Error, net::SocketAddr};
use tokio::{io, net::TcpListener};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

/// Possible requests our clients can send us
enum Request {
    Create {
        pile: String,
        data: String,
    },
    Ping {},
    Find {
        pile: String,
        field: String,
        compare: String,
    },
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(2, ' ');
        match parts.next() {
            Some("CREATE") => {
                let split_input = parts.next().unwrap();
                parts = split_input.splitn(2, ' ');

                let pile = match parts.next() {
                    Some(pile) => pile,
                    None => return Err("CREATE must have a pile name specified".to_owned()),
                };

                let data = match parts.next() {
                    Some(data) => data,
                    None => return Err("CREATE must have data after the pile name".to_owned()),
                };

                Ok(Request::Create {
                    pile: pile.to_string().to_lowercase(),
                    data: data.to_string(),
                })
            }
            Some("PING") => Ok(Request::Ping {}),
            Some("FIND") => {
                let split_input = parts.next().unwrap();
                parts = split_input.splitn(3, ' ');

                let pile = match parts.next() {
                    Some(pile) => pile,
                    None => return Err("FIND must have a pile name specified".to_owned()),
                };

                let field = match parts.next() {
                    Some(field) => field,
                    None => {
                        return Err("FIND must have a field name after the pile name".to_owned())
                    }
                };

                let compare = match parts.next() {
                    Some(compare) => compare,
                    None => {
                        return Err("FIND must have a compare name after the field name".to_owned())
                    }
                };

                Ok(Request::Find {
                    pile: pile.to_string().to_lowercase(),
                    field: field.to_string(),
                    compare: compare.to_string(),
                })
            }
            Some(cmd) => Err(format!("Error parsing request, unknown command: {}", cmd)),
            None => Err("Error parsing request, empty request".to_owned()),
        }
    }
}

/// Responses to the `Request` commands above
enum Response {
    Ok {
        exit_code: u8,
        message: Option<String>,
    },
    Error {
        exit_code: u8,
        error: String,
    },
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Ok {
                ref exit_code,
                ref message,
            } => format!(
                "{} {}",
                exit_code,
                match message {
                    None => "",
                    Some(message) => message,
                }
            ),
            Response::Error {
                ref exit_code,
                ref error,
            } => format!("{} Error: {}", exit_code, error),
        }
    }
}

//https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = format!(
        "{}:{}",
        get_env_var("DUST_DB_ADDR"),
        get_env_var("DUST_DB_PORT")
    );
    let listener = TcpListener::bind(&addr).await?;
    println!("dustdb successfully started, listening on: {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, socket_addr)) => {
                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut lines = Framed::new(socket, LinesCodec::new());

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &socket_addr);
                                let response = response.serialize();

                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("Error sending response: {:?}", e);
                                }

                                // We only accept once command at a time -- never a persistent connection
                                break;
                            }
                            Err(e) => {
                                println!("Error decoding from socket: {:?}", e);
                            }
                        }
                    }

                    // The connection will be closed at this point as `lines.next()` has returned `None`.
                });
            }
            Err(e) => println!("Error accepting socket: {:?}", e),
        }
    }
}

fn handle_request(line: &str, socket_addr: &SocketAddr) -> Response {
    let request = match Request::parse(line) {
        Ok(req) => {
            capture_request_log(
                LogLevel::INFO,
                socket_addr,
                String::from(line),
                Some(size_of_val(&*line)),
            );

            req
        }
        Err(e) => {
            capture_request_log(
                LogLevel::ERROR,
                socket_addr,
                String::from(line),
                Some(size_of_val(&*line)),
            );

            return response_handler(Response::Error {
                exit_code: 1,
                error: e,
            });
        }
    };

    match request {
        Request::Create { pile, data } => match create(&pile, &data) {
            Ok(generated_uuid) => response_handler(Response::Ok {
                exit_code: 0,
                message: Some(generated_uuid),
            }),
            Err(e) => response_handler(Response::Error {
                exit_code: 1,
                error: format!("Error creating database entry: {}", e),
            }),
        },
        Request::Ping {} => response_handler(Response::Ok {
            exit_code: 0,
            message: None,
        }),
        Request::Find {
            pile,
            field,
            compare,
        } => match find(&pile, &field, &compare) {
            Ok(encoded_json_data) => response_handler(Response::Ok {
                exit_code: 0,
                message: Some(encoded_json_data),
            }),
            Err(e) => response_handler(Response::Error {
                exit_code: 1,
                error: format!("Error finding database entry: {}", e),
            }),
        },
    }
}

fn response_handler(response: Response) -> Response {
    match response {
        Response::Ok {
            ref exit_code,
            ref message,
        } => {
            let log = DBResponseLog {
                timestamp: Utc::now(),
                log_level: LogLevel::INFO,
                exit_code: exit_code.clone(),
                message: message.clone(),
            };

            match write_to_log(log.as_log_str(), log.get_log_distinction()) {
                Ok(_) => (),
                Err(e) => eprintln!("{:?}", e),
            };

            response
        }
        Response::Error {
            ref exit_code,
            ref error,
        } => {
            let log = DBResponseLog {
                timestamp: Utc::now(),
                log_level: LogLevel::ERROR,
                exit_code: exit_code.clone(),
                message: Some(error.clone()),
            };

            match write_to_log(log.as_log_str(), log.get_log_distinction()) {
                Ok(_) => (),
                Err(e) => eprintln!("{:?}", e),
            };

            response
        }
    }
}

// Example:
/// in: FIND users email matthew@saplink.io
/// out: 7ABC07ABC07ABC07ABC07ABC07ABC07ABC07ABC07ABC0
fn find(pile_name: &str, field_name: &str, compare_name: &str) -> Result<String, io::Error> {
    let pile_path = format!("{}{}", get_env_var("DUST_DATA_STORAGE_PATH"), &pile_name);
    let dir_path = Path::new(&pile_path);
    if dir_path.is_dir() {
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let file_content = fs::read_to_string(entry.path())?;
            let json_content: Value = from_str(&file_content)?;
            if let Some(value) = json_content.get(field_name) {
                if value.as_str().unwrap() == compare_name {
                    let encoded_json_data = encode_utf8_to_hex(&file_content);
                    return Ok(encoded_json_data);
                }
            }
        }
    }
    // Do not want an error if pile doesn't exist, this was for testing only.
    // If the pile doesn't exist, no data to return!
    // else {
    //     let e_kind = io::ErrorKind::NotFound;
    //     let e = format!("Could not find pile: \"{}\"", pile_name).to_owned();
    //     let error = io::Error::new(e_kind, e);
    //     return Err(error);
    // }
    Ok(String::new())
}

/// Example:
/// in: CREATE users 7ABC07ABC07ABC07ABC07ABC07ABC07ABC07ABC07ABC0
/// out: cd8abd45-ad36-4cf6-a520-c1c5d0671d96
///
/// NOTE: We are writing the PLAIN TEXT DATA to the file! This makes it easier
/// for future viewing via filesystem/other ops. This is a security trade-off:
/// the logic here is that if a potential, bad actor already has access to the
/// filesystem, then the data being encoded as plaintext vs. hex does not really
/// make a difference in the grand scheme of security. :)
fn create(pile_name: &str, data_as_hex_string: &str) -> Result<String, io::Error> {
    // STEP 1: Generate a UUID to be used for future ops
    // TODO: Check for uuid collision ?
    let generated_uuid: String = generate_v4_uuid();

    // STEP 2: Decode the data back into plaintext (from hex)
    let decoded_data_result = match decode_hex_to_utf8(&data_as_hex_string) {
        Ok(utf8_string) => Ok(utf8_string),
        Err(e) => Err(e),
    }?;

    // STEP 3: Create the path for the desired pile (if not exists)
    let pile_path = format!("{}{}", get_env_var("DUST_DATA_STORAGE_PATH"), &pile_name);
    match fs::create_dir_all(&pile_path) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }?;

    // STEP 4: Write the decoded data into the pile
    let file_path = format!(
        "{}/{}.{}",
        pile_path,
        generated_uuid,
        get_env_var("DUST_DATA_FMT")
    );

    match fs::write(&file_path, decoded_data_result) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }?;

    Ok(generated_uuid)
}

fn capture_request_log(
    log_level: LogLevel,
    socket_addr: &SocketAddr,
    command: String,
    payload_size_in_bytes: Option<usize>,
) {
    let log = DBRequestLog {
        timestamp: Utc::now(),
        log_level,
        socket_addr: socket_addr.to_string(),
        command,
        payload_size_in_bytes,
    };

    match write_to_log(log.as_log_str(), log.get_log_distinction()) {
        Ok(_) => (),
        Err(e) => eprintln!("{:?}", e),
    }
}

/// DustDB v0.1.0
/// Matthew Roy <matthew@saplink.io>
///
/// Operations supported:
///
/// 1. [C]reate in storage.
/// 2. [R]ead from storage.
/// 3. [U]pdate data already in storage.
/// 4. [D]elete from storage.
use dustcfg::{decode_hex_to_utf8, generate_v4_uuid, get_env_var};
use futures::SinkExt;
use std::error::Error;
use std::fs;
use tokio::{io, net::TcpListener};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

const SERVER_ADDR: &str = "127.0.0.1";

/// Possible requests our clients can send us
enum Request {
    Create { pile: String, data: String },
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, ' ');
        match parts.next() {
            Some("CREATE") => {
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

            Some(cmd) => Err(format!("Error parsing request, unknown command: {}", cmd)),
            None => Err("Error parsing request, empty request".to_owned()),
        }
    }
}

/// Responses to the `Request` commands above
enum Response {
    Ok { message: String },
    Error { error: String },
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Ok { ref message } => format!("{}", message),
            Response::Error { ref error } => format!("Error: {}", error),
        }
    }
}

//https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = format!("{}:{}", SERVER_ADDR, get_env_var("DUST_DB_PORT"));
    let listener = TcpListener::bind(&addr).await?;
    println!("dustdb successfully started, listening on: {}", addr);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
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
                                let response = handle_request(&line);
                                let response = response.serialize();
                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("Error sending response: {:?}", e);
                                }
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

fn handle_request(line: &str) -> Response {
    let request = match Request::parse(line) {
        Ok(req) => req,
        Err(e) => return Response::Error { error: e },
    };

    match request {
        Request::Create { pile, data } => match create(&pile, &data) {
            Ok(generated_uuid) => Response::Ok {
                message: generated_uuid,
            },
            Err(e) => Response::Error {
                error: format!("Error creating database entry: {}", e),
            },
        },
    }
}

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

use rand::{Rng, RngCore};
use std::io::{Error, ErrorKind, Result};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use protos::helloworld::greeter_client::GreeterClient;
use protos::helloworld::greeter_server::{Greeter, GreeterServer};
use protos::helloworld::{HelloReply, HelloRequest};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic::{transport::Server, Request, Response, Status};
use tower::timeout::Timeout;
use tskv::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use tskv::file_system::{self, FileSystem};

mod memory;

fn now_timestamp_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub struct DropTest {}

impl Drop for DropTest {
    fn drop(&mut self) {
        println!("{} Droper droped...", now_timestamp_millis());
    }
}

pub(crate) async fn asyncify<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!("background task failed: {:?}", e),
        )),
    }
}

fn check_err_size(e: libc::ssize_t) -> Result<usize> {
    if e == -1_isize {
        Err(Error::last_os_error())
    } else {
        Ok(e as usize)
    }
}

pub fn libc_pread(raw_fd: usize, pos: u64, len: usize, ptr: u64) -> Result<usize> {
    check_err_size(unsafe {
        libc::pread(
            raw_fd as std::os::fd::RawFd,
            ptr as *mut _,
            len as _,
            pos as libc::off_t,
        )
    })
}

async fn read_file1(file: Arc<std::fs::File>, pos: u64, data: &mut [u8]) -> Result<usize> {
    let len = data.len();
    let ptr = data.as_ptr() as u64;
    let fd = file.as_ref().as_raw_fd() as usize;

    let len = asyncify(move || {
        // let path =
        //     Path::new("/Users/cnosdb/github.com/cnosdb/test_data-iot-seed-123-scale-100-15d.zip");
        // let file = std::fs::File::open(path).unwrap();
        // let fd = file.as_raw_fd() as usize;
        // let mut data: Vec<u8> = vec![0_u8; 2 * 1024 * 1024 * 1024 - 1];
        // let len = data.len();
        // let ptr = data.as_ptr() as u64;

        let mut result = Ok(10);
        for i in 0..100 {
            result = libc_pread(fd, pos, len, ptr);
            println!(
                "{} over read.... {:?} {} {}",
                now_timestamp_millis(),
                result,
                i,
                ptr
            );
        }
        result
    })
    .await?;
    Ok(len)

    //libc_pread(fd, pos, len, ptr)
}

async fn read_file2(file: Arc<std::fs::File>, pos: u64, len: usize) -> Result<Vec<u8>> {
    asyncify(move || {
        let fd = file.as_ref().as_raw_fd() as usize;
        let buf: Vec<u8> = vec![0; len];
        let ptr: u64 = buf.as_ptr() as u64;

        libc_pread(fd, pos, len, ptr)?;
        Ok(buf)
    })
    .await
}

async fn read_data_from_file(file: Arc<std::fs::File>) -> usize {
    let _drop = DropTest {};
    let mut buf: Vec<u8> = vec![0_u8; 2 * 1024 * 1024 * 1024 - 1];

    println!("----- * before read");
    let len = read_file1(file, 0, &mut buf).await.unwrap();
    //let data = read_file2(file, 0, 3 * 1024 * 1024 * 1024).await.unwrap();
    //buf.copy_from_slice(&data);
    println!("----- * after read");

    len
    //data.len()
}

async fn test_read() {
    let cancel: CancellationToken = CancellationToken::new();

    let file_path =
        PathBuf::from("/Users/cnosdb/github.com/cnosdb/test_data-iot-seed-123-scale-100-15d.zip");
    let file = Arc::new(std::fs::File::open(file_path).unwrap());

    println!("----- begin test ------------");
    let can_tok = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = can_tok.cancelled() => {
                    println!("----- cancelled break loop {:?}", std::time::SystemTime::now());
                    break;
                }

                 res = read_data_from_file(file.clone()) => {
                    println!("----- read data len: {}",res);
                 }
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    cancel.cancel();
    println!(" ----- cancel.cancel()");

    tokio::time::sleep(tokio::time::Duration::from_millis(5 * 1000)).await;
    println!("----- test over");
}

#[cfg(unix)]
#[global_allocator]
static A: memory::DebugMemoryAlloc = memory::DebugMemoryAlloc;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .thread_stack_size(4 * 1024 * 1024)
        .build()
        .unwrap();

    rt.spawn(async move {
        //test_read().await;
        grpc_server().await;
    });

    std::thread::sleep(std::time::Duration::from_secs(1));

    rt.block_on(async move {
        grpc_request(100).await;
    });

    std::thread::sleep(std::time::Duration::from_secs(10));
}

async fn read_data_from_file2(file: &file_system::file::stream_reader::FileStreamReader) -> usize {
    let _drop = DropTest {};
    let mut buf: Vec<u8> = vec![0_u8; 2 * 1024 * 1024 * 1024 - 1];

    println!("----- * before read");
    let read_size = file.read_at(0, &mut buf).await.unwrap();
    println!("----- * after read");

    read_size
    //data.len()
}

#[derive(Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> core::result::Result<Response<HelloReply>, Status> {
        println!(
            "{} Got a request from {:?}",
            now_timestamp_millis(),
            request.remote_addr()
        );

        let _drop = DropTest {};

        let path =
            Path::new("/Users/cnosdb/github.com/cnosdb/test_data-iot-seed-123-scale-100-15d.zip");
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);

        let mut buf = vec![0_u8; 2 * 1024 * 1024 * 1024 - 1];
        let file = std::fs::File::open(path).unwrap();
        println!("{} before read...", now_timestamp_millis());
        let read_size = read_data_from_file(Arc::new(file)).await;
        println!("{} after  read...", now_timestamp_millis());

        // let file = file_system.open_file_reader(path).await.unwrap();
        // let read_size = read_data_from_file2(&file).await;

        // println!("{} before read...", now_timestamp_millis());
        // let read_size = file.read_at(0, &mut buf).await.unwrap();
        // println!("{} after  read...", now_timestamp_millis());

        let reply = protos::helloworld::HelloReply {
            message: format!("Hello {}: size: {}!", request.into_inner().name, read_size),
        };
        println!("{} Response data", now_timestamp_millis());

        Ok(Response::new(reply))
    }
}

async fn grpc_server() {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let greeter = MyGreeter::default();
    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await
        .unwrap();
}

async fn grpc_request(timeout_ms: u64) {
    let connector = Endpoint::from_shared("http://127.0.0.1:50051").unwrap();
    let channel = connector.connect().await.unwrap();
    let timeout_channel = Timeout::new(channel, std::time::Duration::from_millis(timeout_ms));
    let mut client = GreeterClient::<Timeout<Channel>>::new(timeout_channel);

    let request = tonic::Request::new(HelloRequest {
        name: "Tonic".into(),
    });

    println!("{} Begin request -------", now_timestamp_millis());
    let response = client.say_hello(request).await;
    println!(
        "{} End request --------- RESPONSE={:?}",
        now_timestamp_millis(),
        response
    );
}

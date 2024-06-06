use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use rand::Rng;

use crate::file_system::file;
use crate::file_system::file::os;

#[derive(Debug, Clone)]
#[cfg(not(feature = "io_uring"))]
pub struct RawFile(pub(crate) Arc<File>);

#[derive(Debug, Clone)]
#[cfg(feature = "io_uring")]
struct RawFile(Arc<File>, Arc<rio::Rio>);

fn now_timestamp_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
impl RawFile {
    pub(crate) fn file_size(&self) -> io::Result<usize> {
        os::file_size(os::fd(self.0.as_ref()))
    }
    pub(crate) async fn read_all_at(&self, pos: usize, mut buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.read_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        unsafe {
            let mut pos = pos;
            file::asyncify(|| {
                for i in 0..100 {
                    self.pread(pos, buf).unwrap();

                    println!("{} over read.... {}", now_timestamp_millis(), i);
                }

                Ok(10)
            })
            .await
        }
    }

    pub(crate) async fn write_all_at(&self, pos: usize, mut buf: &[u8]) -> io::Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.1.write_at(&self.0, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        unsafe {
            let mut pos = pos;
            let len = buf.len();
            file::asyncify(|| {
                while !buf.is_empty() {
                    match self.pwrite(pos, buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let tmp = buf;
                            buf = &tmp[n..];
                            pos = pos.checked_add(n).unwrap();
                        }
                        Err(e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(len)
            })
            .await
        }
    }
    fn pwrite(&self, pos: usize, data: &[u8]) -> io::Result<usize> {
        let len = data.len();
        let ptr = data.as_ptr() as u64;
        let fd = os::fd(self.0.as_ref());
        os::pwrite(fd, pos, len, ptr)
    }

    fn pread(&self, pos: usize, data: &mut [u8]) -> io::Result<usize> {
        let len = data.len();
        let ptr = data.as_ptr() as u64;
        let fd = os::fd(self.0.as_ref());
        os::pread(fd, pos, len, ptr)
    }

    pub(crate) async fn sync_data(&self) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.1.fsync(&self.0).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        unsafe {
            let file = self.0.clone();
            file::asyncify(|| file.sync_data()).await
        }
    }

    pub(crate) async fn sync_all(&self) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.1.fsync(&self.0).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        unsafe {
            let file = self.0.clone();
            file::asyncify(|| file.sync_all()).await
        }
    }

    pub(crate) async fn truncate(&self, size: u64) -> io::Result<()> {
        #[cfg(feature = "io_uring")]
        {
            let file = self.0.clone();
            asyncify(move || file.set_len(size)).await
        }
        #[cfg(not(feature = "io_uring"))]
        unsafe {
            let file = self.0.clone();
            file::asyncify(|| file.set_len(size)).await
        }
    }
}

pub struct FsRuntime {
    #[cfg(feature = "io_uring")]
    rio: Arc<Rio>,
}

unsafe impl Send for FsRuntime {}

impl FsRuntime {
    #[allow(dead_code)]
    pub fn new_runtime() -> Self {
        #[cfg(feature = "io_uring")]
        {
            let rio = Arc::new(rio::new().unwrap());
            FsRuntime { rio }
        }
        #[cfg(not(feature = "io_uring"))]
        {
            FsRuntime {}
        }
    }
}

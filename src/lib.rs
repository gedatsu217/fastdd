use clap::Parser;
use std::fs::OpenOptions;
use io_uring::{opcode, types, IoUring};
use std::os::fd::AsRawFd;
use std::collections::VecDeque;
use libc::iovec;
use libc::{rlimit, getrlimit, RLIMIT_MEMLOCK};
use std::sync::{Arc, atomic::{AtomicU64, Ordering, AtomicBool}};
use std::io::{Write};

const TAG_READ: u64 = 0;
const TAG_WRITE: u64 = 1;


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Input file path
    #[arg(long="if")]
    input_file: String,

    /// Output file path
    #[arg(long="of")]
    output_file: String,

    /// Block size
    #[arg(long="bs", default_value_t = 4096)]
    block_size: usize,

    /// Number of blocks to copy. If not specified, the entire file will be copied.
    #[arg(short, long)]
    count: Option<u64>,

    /// Input file seek offset in blocks
    #[arg(long="is", default_value_t = 0)]
    input_seek: u64,

    /// Output file seek offset in blocks
    #[arg(long="os", default_value_t = 0)]
    output_seek: u64,

    /// Size of the io_uring ring. If not specified, a default size will be used.
    #[arg(short, long)]
    ring_size: Option<u32>,

    /// Number of buffers to use. If not specified, a default number will be used.
    #[arg(short, long)]
    num_buffers: Option<u64>,

    /// Show progress during the operation
    #[arg(long, default_value_t = false)]
    progress: bool,
    
}

pub struct ArgData {
    pub ifile: std::fs::File,
    pub ofile: std::fs::File,
    pub block_size: usize,
    pub count: Option<u64>,
    pub iseek: u64,
    pub oseek: u64,
    pub ring_size: u32,
    pub num_buffers: u64,
    pub progress: bool,
}

#[derive(Debug, Clone, Copy)]
struct RWMetadata {
    offset: u64,
    size: u64,
}

fn open_file(path: &str) -> std::io::Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

#[inline]
fn push_sqe(ring: &mut IoUring, sqe: &io_uring::squeue::Entry) -> std::io::Result<()> {
    loop {
        match unsafe { ring.submission().push(sqe) } {
            Ok(()) => return Ok(()),
            Err(_) => ring.submit()?,
        };
    }
}

fn get_memlock_limit() -> Option<u64> {
    let mut lim = rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    let res = unsafe { getrlimit(RLIMIT_MEMLOCK, &mut lim) };
    if res == 0 {
        Some(lim.rlim_cur as u64)
    } else {
        None
    }
}

fn print_status(cur_bytes: u64, total_size: u64) {
    if total_size > 0 {
        let percent = (cur_bytes as f64 / total_size as f64) * 100.0;
        eprint!("\rProgress: {:.2}%", percent);
    } else {
        eprint!("\rProgress: N/A");
    }
    std::io::stdout().flush().unwrap();
}

pub fn execute_dd(arg_data: &ArgData) -> std::io::Result<u64> {
    let mut uring = IoUring::new(256)?;
    let ifile = &arg_data.ifile;
    let ofile = &arg_data.ofile;
    let bs = arg_data.block_size as u64;
    let ibase = arg_data.iseek * bs;
    let obase = arg_data.oseek * bs;
    let num_buffers = arg_data.num_buffers;
    let mut free_bufs: VecDeque<u64> = (0..num_buffers).collect();
    let default_metadata = RWMetadata { offset: 0, size: 0 };
    let mut metadata = vec![default_metadata; (num_buffers * 2) as usize];
    let file_len = ifile.metadata()?.len().checked_sub(ibase)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Invalid input seek offset"))?;
    let total_size = match arg_data.count {
        Some(c) => file_len.min(c * bs),
        None => {
            file_len
        }
    };
    let mut cur_blocks = 0;
    let num_blocks = if total_size % bs == 0 {total_size / bs} else {total_size / bs + 1};
    let mut cur_bytes = 0;
    let mut to_reads: VecDeque<(u64, u64)> = VecDeque::new(); // offset & size. Used for reading remaining data

    let max_buf_size = match get_memlock_limit() {
        Some(limit) => limit,
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to get memory lock limit",
            ));
        }
    };

    let registered_num_buffers = if num_buffers * bs as u64 >= max_buf_size {
        (max_buf_size / bs as u64) as u64 - 1
    } else {
        num_buffers
    };

    let mut bufs: Vec<Box<[u8]>> = (0..num_buffers)
        .map(|_| vec![0u8; bs as usize].into_boxed_slice())
        .collect();

    let iovecs: Vec<iovec> = bufs.iter()
        .take(registered_num_buffers as usize)
        .map(|buf| iovec {
            iov_base: buf.as_ptr() as *mut _,
            iov_len: buf.len(),
        })
        .collect();

    let fds = [ifile.as_raw_fd(), ofile.as_raw_fd()];

    unsafe {
        uring.submitter().register_buffers(&iovecs)?;
        uring.submitter().register_files(&fds)?;
    }

    let status_byte_count = Arc::new(AtomicU64::new(0));
    let status_byte_count_clone = Arc::clone(&status_byte_count);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);
    let handle = if !arg_data.progress {
        None
    } else {
        Some(std::thread::spawn(move || {
            while !stop_flag_clone.load(Ordering::Relaxed) {
                let bytes_copied = status_byte_count_clone.load(Ordering::Relaxed);
                print_status(bytes_copied, total_size);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }))
    };

    // main loop
    while cur_blocks < num_blocks {
        while !free_bufs.is_empty() && (!to_reads.is_empty() || cur_bytes < total_size) {
            if uring.submission().is_full() {
                uring.submit()?;
            }
            let (roffset, rsize);
            if !to_reads.is_empty() {
                (roffset, rsize) = to_reads.pop_front().unwrap();
            } else if cur_bytes < total_size {
                roffset = ibase + cur_bytes;
                rsize = bs.min(total_size - cur_bytes);
                cur_bytes += rsize;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "No more data to read",
                ));
            }

            let buf_idx = free_bufs.pop_front().unwrap();
            let buf = &mut bufs[buf_idx as usize];
            let user_data = buf_idx << 1 | TAG_READ;
            let read_op = if buf_idx < registered_num_buffers {
                opcode::ReadFixed::new(
                    types::Fixed(0),
                    buf.as_mut_ptr(),
                    rsize as u32,
                    buf_idx as u16,
                ).offset(roffset).build().user_data(user_data)
            } else {
                opcode::Read::new(
                    types::Fixed(0),
                    buf.as_mut_ptr(),
                    rsize as u32,
                ).offset(roffset).build().user_data(user_data)
            };
            push_sqe(&mut uring, &read_op)?;
            metadata[buf_idx as usize * 2] = RWMetadata { offset: roffset, size: rsize };
        }

        loop{
            if uring.completion().is_empty() {
                uring.submit_and_wait(1)?;
            }
            let cqe = uring.completion().next().expect("Failed to get completion event");
            let op_type = cqe.user_data() & 1;
            let buf_idx = cqe.user_data() >> 1;
            let res = cqe.result();
            if op_type == TAG_READ {
                if res < 0 {
                    eprintln!("Read operation failed: {}", res);
                    return Err(std::io::Error::from_raw_os_error(-res as i32));
                }
                let res = res as u64;
                let read_metadata = &metadata[buf_idx as usize * 2];
                // if res as u64 == 0 {
                //     eprintln!("Read should not be zero here");
                //     return 1;
                // }
                if res != read_metadata.size {
                    assert!(res < read_metadata.size);
                    let roffset = read_metadata.offset + res;
                    let rsize = read_metadata.size - res;
                    to_reads.push_back((roffset, rsize));
                }
                let woffset = obase + read_metadata.offset - ibase;
                let user_data = buf_idx << 1 | TAG_WRITE;
                let write_op = if buf_idx < registered_num_buffers {
                    opcode::WriteFixed::new(
                        types::Fixed(1),
                        bufs[buf_idx as usize].as_ptr(),
                        res as u32,
                        buf_idx as u16,
                    ).offset(woffset).build().user_data(user_data)
                } else {
                    opcode::Write::new(
                        types::Fixed(1),
                        bufs[buf_idx as usize].as_ptr(),
                        res as u32,
                    ).offset(woffset).build().user_data(user_data)
                };
                push_sqe(&mut uring, &write_op)?;
                metadata[buf_idx as usize * 2 + 1] = RWMetadata { offset: woffset, size: res };
            } else { // TAG_WRITE
                if res < 0 {
                    eprintln!("Write operation failed: {}", res);
                    return Err(std::io::Error::from_raw_os_error(-res as i32));
                }
                let res = res as u64;
                let write_metadata = &metadata[buf_idx as usize * 2 + 1];
                status_byte_count.fetch_add(res, Ordering::Relaxed);
                if res != write_metadata.size {
                    assert!(res < write_metadata.size);
                    let woffset = write_metadata.offset + res;
                    let wsize = write_metadata.size - res;
                    let user_data = buf_idx << 1 | TAG_WRITE;
                    let remaining_buf_ptr = unsafe{bufs[buf_idx as usize].as_ptr().add(res as usize)};
                    let write_op = if buf_idx < registered_num_buffers {
                        opcode::WriteFixed::new(
                            types::Fixed(1),
                            remaining_buf_ptr,
                            wsize as u32,
                            buf_idx as u16,
                        ).offset(woffset).build().user_data(user_data)
                    } else {
                        opcode::Write::new(
                            types::Fixed(1),
                            remaining_buf_ptr,
                            wsize as u32,
                        ).offset(woffset).build().user_data(user_data)
                    };

                    push_sqe(&mut uring, &write_op)?;
                    
                    metadata[buf_idx as usize * 2 + 1] = RWMetadata { offset: woffset, size: wsize };
                } else {
                    cur_blocks += 1;
                    free_bufs.push_back(buf_idx);
                }
            }
            if uring.completion().is_empty() {
                break;
            }
        }
        if !uring.submission().is_empty() {
            uring.submit()?;
        }
    }
    
    uring.submitter().unregister_buffers()?;
    uring.submitter().unregister_files()?;
    stop_flag.store(true, Ordering::Relaxed);
    if let Some(handle) = handle {
        handle.join().expect("Failed to join progress thread");
        eprintln!("\rProgress: 100.00% done");
    }
    
    Ok(total_size)
}


pub fn arg_parse() -> ArgData {
    let args = Args::parse();
    let input_file = open_file(&args.input_file).expect("Failed to open input file");
    let output_file = open_file(&args.output_file).expect("Failed to open output file");
    let (ring_size, num_buffers) = match (args.ring_size, args.num_buffers) {
        (Some(r), Some(n)) => {
            if r == 0 || n == 0 {
                panic!("Ring size and number of buffers must be greater than 0");
            }
            (r, n)
        },
        (Some(r), None) => {
            if r == 0 {
                panic!("Ring size must be greater than 0");
            }
            (r, if r == 1 { 1 } else { r as u64 / 2 })
        },
        (None, Some(n)) => {
            if n == 0 {
                panic!("Number of buffers must be greater than 0");
            }
            (n as u32 * 2, n)
        },
        (None, None) => (256, 128), // Default ring size and number of buffers
    };

    if args.block_size == 0 {
        panic!("Block size must be greater than 0");
    }

    ArgData {
        ifile: input_file,
        ofile: output_file,
        block_size: args.block_size,
        count: args.count,
        iseek: args.input_seek,
        oseek: args.output_seek,
        ring_size: ring_size,
        num_buffers: num_buffers,
        progress: args.progress,
    }
}

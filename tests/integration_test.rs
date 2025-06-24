use rand::Rng;

#[test]
fn copy_random_5m() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let src = tmp.path().join("src.bin");
    let dst = tmp.path().join("dst.bin");

    let mut rng = rand::rng();
    let data: Vec<u8> = (0..5 * 1024 * 1024).map(|_| rng.random::<u8>()).collect();
    std::fs::write(&src, &*data)?;
    
    let argdata = fastdd::ArgData {
        ifile: std::fs::File::open(&src)?,
        ofile: std::fs::File::create(&dst)?,
        block_size: 4096,
        count: None,
        iseek: 0,
        oseek: 0,
        ring_size: 256,
        num_buffers: 128,
    };

    let result = fastdd::execute_dd(&argdata);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 5 * 1024 * 1024);
    
    assert_eq!(md5::compute(std::fs::read(src)?),
               md5::compute(std::fs::read(dst)?));
    Ok(())
}
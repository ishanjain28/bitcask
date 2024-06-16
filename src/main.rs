use bitcask::{BitCask, BitCaskOptions};
use rand::Rng;

fn main() {
    let mut cask = BitCask::open(BitCaskOptions {
        dir_name: "db".to_string(),
        ..Default::default()
    })
    .expect("error in opening db");
    println!("started up");

    cask.put("name", "ishan jain")
        .expect("error in writing record");
    cask.put("name", "ishan jain")
        .expect("error in writing record");
    cask.put("name", "ishan jain22")
        .expect("error in writing record");
    cask.put("name2", "ishanasdasdsadad wjain2")
        .expect("error in writing record");

    let mut rng = rand::thread_rng();

    // for _ in 0..100000 {
    //     let mut key = vec![0; 32];
    //     let mut value = vec![0; 512];
    //     let key_size = rng.gen_range(0..32);
    //     let value_size = rng.gen_range(32..512);

    //     rng.fill(&mut key[..key_size]);
    //     rng.fill(&mut value[..value_size]);

    //     cask.put(&key[..key_size], &value[..value_size])
    //         .expect("error in writing record");
    // }

    let resp = cask.get("name2").expect("error in getting key");
    println!("{}", resp);

    let resp = cask.get("name").expect("error in getting key");
    println!("{}", resp);

    cask.close();
}

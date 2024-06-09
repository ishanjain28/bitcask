use bitcask::BitCask;

fn main() {
    let mut cask = BitCask::open("db").expect("error in opening db");

    cask.put("name", "ishan jain")
        .expect("error in writing record");
    cask.put("name", "ishan jain")
        .expect("error in writing record");
    cask.put("name", "ishan jain22")
        .expect("error in writing record");
    cask.put("name2", "ishan wjain2")
        .expect("error in writing record");

    cask.read_all_and_seed_keydir();

    cask.close();
}

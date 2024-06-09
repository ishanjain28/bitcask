use bitcask::BitCask;

fn main() {
    let mut cask = BitCask::open("db").expect("error in opening db");

    cask.put("name", "ishan jain")
        .expect("error in writing record");

    cask.close();
}

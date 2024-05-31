use bitcask::BitCask;

fn main() {
    let mut cask = BitCask::open("db");

    cask.put("asdsad", "askjldhakdlsa");

    cask.close();
}

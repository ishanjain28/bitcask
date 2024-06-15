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

    let resp = cask.get("name2").expect("error in getting key");
    println!("{}", resp);

    let resp = cask.get("name").expect("error in getting key");
    println!("{}", resp);

    cask.close();
}

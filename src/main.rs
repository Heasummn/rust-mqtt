mod headers;
use headers::fixed_header::FixedHeader;

fn main() {
    let data = 0b1111111;
    let header = FixedHeader::new(data);
    println!("{}",header.packet_type())
}

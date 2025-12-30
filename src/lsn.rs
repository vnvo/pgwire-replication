use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn parse(s: &str) -> Option<Lsn> {
        let mut parts = s.split('/');
        let hi = u64::from_str_radix(parts.next()?, 16).ok()?;
        let lo = u64::from_str_radix(parts.next()?, 16).ok()?;
        Some(Lsn((hi << 32) | lo))
    }

    pub fn to_pg_string(self) -> String {
        format!("{:X}/{:X}", (self.0 >> 32) as u32, self.0 as u32)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_pg_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Lsn;

    #[test]
    fn lsn_parse_roundtrip() {
        let s = "16/B374D848";
        let l = Lsn::parse(s).unwrap();
        assert_eq!(l.to_pg_string(), s);
    }
}

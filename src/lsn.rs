use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseLsnError(pub String);

impl std::fmt::Display for ParseLsnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid LSN: {}", self.0)
    }
}
impl std::error::Error for ParseLsnError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn parse(s: &str) -> Result<Lsn, ParseLsnError> {
        let mut parts = s.split('/');
        let hi = parts.next().ok_or_else(|| ParseLsnError(s.into()))?;
        let lo = parts.next().ok_or_else(|| ParseLsnError(s.into()))?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| ParseLsnError(s.into()))?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| ParseLsnError(s.into()))?;
        Ok(Lsn((hi << 32) | lo))
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

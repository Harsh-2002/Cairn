//! Browser download names are presentation metadata, independent of object storage and share URLs.

use std::fmt::Write;

use cairn_types::meta::ShareDisposition;
use http::HeaderValue;

/// Discard path components and controls, but retain the name and extension rather than guessing
/// them from the object's MIME type. An unusable custom name falls back to the object's basename.
fn basename(value: &str) -> Option<String> {
    let name: String = value
        .rsplit(['/', '\\'])
        .next()?
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    let name = name.trim();
    (!name.is_empty() && !matches!(name, "." | "..")).then(|| name.to_owned())
}

pub(crate) fn disposition(
    delivery: ShareDisposition,
    custom_name: Option<&str>,
    key: &str,
) -> HeaderValue {
    let name = custom_name
        .and_then(basename)
        .or_else(|| basename(key))
        .unwrap_or_else(|| "download".to_owned());
    let delivery = match delivery {
        ShareDisposition::Inline => "inline",
        ShareDisposition::Attachment => "attachment",
    };
    let mut value = format!("{delivery}; filename=\"");
    for c in name.chars() {
        // Some older clients percent-decode filename even though RFC 6266 forbids that. The
        // extended parameter below carries the exact name, including literal percent signs.
        if !c.is_ascii() || c == '%' {
            value.push('_');
        } else {
            if c == '"' {
                value.push('\\');
            }
            value.push(c);
        }
    }
    value.push_str("\"; filename*=UTF-8''");
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            value.push(char::from(b));
        } else {
            write!(value, "%{b:02X}").expect("writing to a String cannot fail");
        }
    }
    HeaderValue::from_str(&value).expect("disposition contains only safe ASCII header bytes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_unusable_names_use_the_object_basename() {
        for custom in [None, Some(""), Some(" \t"), Some(".."), Some("/")] {
            assert_eq!(
                disposition(ShareDisposition::Attachment, custom, "documents/report.pdf"),
                "attachment; filename=\"report.pdf\"; filename*=UTF-8''report.pdf"
            );
        }
        assert_eq!(
            disposition(ShareDisposition::Inline, None, "folder/"),
            "inline; filename=\"download\"; filename*=UTF-8''download"
        );
    }

    #[test]
    fn unicode_quotes_and_literal_percent_names_round_trip_in_extended_parameter() {
        let value = disposition(
            ShareDisposition::Inline,
            Some("folder/résumé \"100%+\".pdf"),
            "ignored.txt",
        );
        assert_eq!(
            value,
            "inline; filename=\"r_sum_ \\\"100_+\\\".pdf\"; filename*=UTF-8''r%C3%A9sum%C3%A9%20%22100%25+%22.pdf"
        );
        assert_eq!(
            disposition(ShareDisposition::Attachment, None, "literal%2Fname"),
            "attachment; filename=\"literal_2Fname\"; filename*=UTF-8''literal%252Fname"
        );
    }

    #[test]
    fn strips_paths_and_controls_without_adding_an_extension() {
        assert_eq!(
            disposition(
                ShareDisposition::Attachment,
                Some("C:\\tmp\\file\r\n\0"),
                "x"
            ),
            "attachment; filename=\"file\"; filename*=UTF-8''file"
        );
    }
}

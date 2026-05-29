use std::fmt::Write;

const SIZE: i32 = 80;
const ELEMENTS: usize = 3;

const COLORS: &[&str] = &[
    "#D4944C", "#E8A85E", "#C27435",
    "#4ADE80", "#2DD4BF",
];

fn hash_code(name: &str) -> u32 {
    let mut hash: i32 = 0;
    for c in name.chars() {
        hash = hash.wrapping_mul(31).wrapping_add(c as i32);
    }
    hash.unsigned_abs()
}

fn get_digit(number: u32, ntn: u32) -> u32 {
    (number / 10u32.pow(ntn)) % 10
}

fn get_unit(number: u32, range: i32, index: Option<u32>) -> i32 {
    let value = (number % range as u32) as i32;
    match index {
        Some(i) if get_digit(number, i).is_multiple_of(2) => -value,
        _ => value,
    }
}

fn get_random_color(number: u32) -> &'static str {
    COLORS[number as usize % COLORS.len()]
}

struct ElementProps {
    color: &'static str,
    translate_x: i32,
    translate_y: i32,
    scale: f64,
    rotate: i32,
}

fn generate_elements(name: &str) -> [ElementProps; ELEMENTS] {
    let num = hash_code(name);
    [
        ElementProps {
            color: get_random_color(num),
            translate_x: 0,
            translate_y: 0,
            scale: 1.0,
            rotate: 0,
        },
        ElementProps {
            color: get_random_color(num + 1),
            translate_x: get_unit(num.wrapping_mul(2), SIZE / 10, Some(1)),
            translate_y: get_unit(num.wrapping_mul(2), SIZE / 10, Some(2)),
            scale: 1.2 + get_unit(num.wrapping_mul(2), SIZE / 20, None) as f64 / 10.0,
            rotate: get_unit(num.wrapping_mul(2), 360, Some(1)),
        },
        ElementProps {
            color: get_random_color(num + 2),
            translate_x: get_unit(num.wrapping_mul(3), SIZE / 10, Some(1)),
            translate_y: get_unit(num.wrapping_mul(3), SIZE / 10, Some(2)),
            scale: 1.2 + get_unit(num.wrapping_mul(3), SIZE / 20, None) as f64 / 10.0,
            rotate: get_unit(num.wrapping_mul(3), 360, Some(1)),
        },
    ]
}

pub fn marble_avatar_svg(name: &str, size: u32) -> String {
    let props = generate_elements(name);
    let mask_id = format!("m{:x}", hash_code(name));
    let filter_id = format!("f{}", &mask_id);
    let cx = SIZE / 2;
    let cy = SIZE / 2;

    let mut svg = String::with_capacity(1500);
    let _ = write!(
        svg,
        r#"<svg viewBox="0 0 {SIZE} {SIZE}" fill="none" xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}">"#
    );

    // Mask for circular clip
    let rx = SIZE * 2;
    let _ = write!(
        svg,
        r#"<mask id="{mask_id}" maskUnits="userSpaceOnUse" x="0" y="0" width="{SIZE}" height="{SIZE}"><rect width="{SIZE}" height="{SIZE}" rx="{rx}" fill="white"/></mask>"#
    );

    // Filter for gaussian blur
    let _ = write!(
        svg,
        r#"<defs><filter id="{filter_id}" filterUnits="userSpaceOnUse" color-interpolation-filters="sRGB"><feFlood flood-opacity="0" result="bg"/><feBlend in="SourceGraphic" in2="bg" result="shape"/><feGaussianBlur stdDeviation="7" result="blur"/></filter></defs>"#
    );

    let _ = write!(svg, r#"<g mask="url(#{mask_id})">"#);

    // Element 0: background
    let _ = write!(
        svg,
        r#"<rect width="{SIZE}" height="{SIZE}" fill="{}"/>"#,
        props[0].color
    );

    // Element 1: first blob
    let _ = write!(
        svg,
        r#"<path filter="url(#{filter_id})" d="M32.414 59.35L50.376 70.5H72.5v-71H33.728L26.5 13.381l19.057 27.08L32.414 59.35z" fill="{}" transform="translate({} {}) rotate({} {} {}) scale({:.2})"/>"#,
        props[1].color,
        props[1].translate_x, props[1].translate_y,
        props[1].rotate, cx, cy,
        props[2].scale
    );

    // Element 2: overlay blob
    let _ = write!(
        svg,
        r#"<path filter="url(#{filter_id})" style="mix-blend-mode:overlay" d="M22.216 24L0 46.75l14.108 38.129L78 86l-3.081-59.276-22.378 4.005 12.972 20.186-23.35 27.395L22.215 24z" fill="{}" transform="translate({} {}) rotate({} {} {}) scale({:.2})"/>"#,
        props[2].color,
        props[2].translate_x, props[2].translate_y,
        props[2].rotate, cx, cy,
        props[2].scale
    );

    svg.push_str("</g></svg>");
    svg
}

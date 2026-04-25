use o_sfu_protocol::signaling::WebSocketCloseCode;

#[derive(Clone, Copy)]
pub(super) struct LabeledValue {
    label_value: &'static str,
    value: u64,
}

impl LabeledValue {
    pub(super) const fn new(label_value: &'static str, value: u64) -> Self {
        Self { label_value, value }
    }
}

#[derive(Clone, Copy)]
pub(super) struct LabeledValue2 {
    first_label_value: &'static str,
    second_label_value: &'static str,
    value: u64,
}

impl LabeledValue2 {
    pub(super) const fn new(
        first_label_value: &'static str,
        second_label_value: &'static str,
        value: u64,
    ) -> Self {
        Self {
            first_label_value,
            second_label_value,
            value,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct LabeledGaugeValue {
    label_value: &'static str,
    value: i64,
}

impl LabeledGaugeValue {
    pub(super) const fn new(label_value: &'static str, value: i64) -> Self {
        Self { label_value, value }
    }
}

#[derive(Clone, Copy)]
pub(super) struct HistogramBucketValue {
    upper_bound: &'static str,
    value: u64,
}

impl HistogramBucketValue {
    pub(super) const fn new(upper_bound: &'static str, value: u64) -> Self {
        Self { upper_bound, value }
    }
}

pub(super) struct LabeledHistogramValue<'a> {
    label_value: &'static str,
    buckets: &'a [HistogramBucketValue],
    sum_micros: u64,
    count: u64,
}

impl<'a> LabeledHistogramValue<'a> {
    pub(super) const fn new(
        label_value: &'static str,
        buckets: &'a [HistogramBucketValue],
        sum_micros: u64,
        count: u64,
    ) -> Self {
        Self {
            label_value,
            buckets,
            sum_micros,
            count,
        }
    }
}

pub(super) fn append_counter(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    output.push_str(name);
    output.push(' ');
    append_u64(output, value);
    output.push('\n');
}

pub(super) fn append_gauge(output: &mut String, name: &str, help: &str, value: i64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" gauge\n");
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

pub(super) fn append_labeled_counter_family(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    values: &[LabeledValue],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        append_u64(output, value.value);
        output.push('\n');
    }
}

pub(super) fn append_labeled_counter_family_2(
    output: &mut String,
    name: &str,
    help: &str,
    label_names: (&str, &str),
    values: &[LabeledValue2],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_names.0);
        output.push_str("=\"");
        output.push_str(value.first_label_value);
        output.push_str("\",");
        output.push_str(label_names.1);
        output.push_str("=\"");
        output.push_str(value.second_label_value);
        output.push_str("\"} ");
        append_u64(output, value.value);
        output.push('\n');
    }
}

pub(super) fn append_labeled_gauge_family(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    values: &[LabeledGaugeValue],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" gauge\n");
    for value in values {
        output.push_str(name);
        output.push('{');
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        output.push_str(&value.value.to_string());
        output.push('\n');
    }
}

pub(super) fn append_histogram(
    output: &mut String,
    name: &str,
    help: &str,
    buckets: &[HistogramBucketValue],
    sum_micros: u64,
    count: u64,
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" histogram\n");
    for bucket in buckets {
        output.push_str(name);
        output.push_str("_bucket{le=\"");
        output.push_str(bucket.upper_bound);
        output.push_str("\"} ");
        append_u64(output, bucket.value);
        output.push('\n');
    }
    output.push_str(name);
    output.push_str("_bucket{le=\"+Inf\"} ");
    append_u64(output, count);
    output.push('\n');
    output.push_str(name);
    output.push_str("_sum ");
    append_seconds_from_micros(output, sum_micros);
    output.push('\n');
    output.push_str(name);
    output.push_str("_count ");
    append_u64(output, count);
    output.push('\n');
}

pub(super) fn append_labeled_histogram_family(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    values: &[LabeledHistogramValue<'_>],
) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push('\n');
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push_str(" histogram\n");
    for value in values {
        for bucket in value.buckets {
            output.push_str(name);
            output.push_str("_bucket{");
            output.push_str(label_name);
            output.push_str("=\"");
            output.push_str(value.label_value);
            output.push_str("\",le=\"");
            output.push_str(bucket.upper_bound);
            output.push_str("\"} ");
            append_u64(output, bucket.value);
            output.push('\n');
        }
        output.push_str(name);
        output.push_str("_bucket{");
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\",le=\"+Inf\"} ");
        append_u64(output, value.count);
        output.push('\n');
        output.push_str(name);
        output.push_str("_sum{");
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        append_seconds_from_micros(output, value.sum_micros);
        output.push('\n');
        output.push_str(name);
        output.push_str("_count{");
        output.push_str(label_name);
        output.push_str("=\"");
        output.push_str(value.label_value);
        output.push_str("\"} ");
        append_u64(output, value.count);
        output.push('\n');
    }
}

fn append_u64(output: &mut String, value: u64) {
    output.push_str(&value.to_string());
}

fn append_seconds_from_micros(output: &mut String, micros: u64) {
    let whole_seconds = micros / 1_000_000;
    let fractional_micros = micros % 1_000_000;
    output.push_str(&whole_seconds.to_string());
    if fractional_micros == 0 {
        output.push_str(".0");
        return;
    }
    output.push('.');
    let mut fractional = format!("{fractional_micros:06}");
    while fractional.ends_with('0') {
        fractional.pop();
    }
    output.push_str(&fractional);
}

pub(super) const fn close_code_label(close_code: WebSocketCloseCode) -> &'static str {
    match close_code {
        WebSocketCloseCode::AuthTimeout => "auth_timeout",
        WebSocketCloseCode::AuthFailed => "auth_failed",
        WebSocketCloseCode::ProtocolError => "protocol_error",
        WebSocketCloseCode::RoomFull => "room_full",
        WebSocketCloseCode::Error => "error",
        WebSocketCloseCode::Clean => "clean",
        WebSocketCloseCode::Leaving => "leaving",
        WebSocketCloseCode::Kicked => "kicked",
    }
}

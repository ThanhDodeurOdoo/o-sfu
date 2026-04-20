use proc_macro::TokenStream;

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Expr, Ident, ItemFn, LitStr, Result, Token,
    parse::{Parse, ParseStream},
};

#[proc_macro_attribute]
pub fn measure_duration(attr: TokenStream, item: TokenStream) -> TokenStream {
    match expand_measured_function(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn measure_http_request(attr: TokenStream, item: TokenStream) -> TokenStream {
    match expand_http_request_function(attr.into(), item.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_measured_function(attr: TokenStream2, item: TokenStream2) -> Result<TokenStream2> {
    let args = syn::parse2::<MeasureDurationArgs>(attr)?;
    let metrics = syn::parse_str::<Expr>(&args.metrics.value())?;
    let record = syn::parse_str::<Ident>(&args.record.value())?;
    let function = syn::parse2::<ItemFn>(item)?;
    let attrs = &function.attrs;
    let vis = &function.vis;
    let sig = &function.sig;
    let block = &function.block;

    Ok(quote! {
        #(#attrs)*
        #vis #sig {
            let __telemetry_metrics = &#metrics;
            let __telemetry_record = |__telemetry_elapsed| {
                __telemetry_metrics.#record(__telemetry_elapsed);
            };
            struct __TelemetryDurationGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                started_at: std::time::Instant,
                record: Option<F>,
            }
            impl<F> Drop for __TelemetryDurationGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                fn drop(&mut self) {
                    if let Some(record) = self.record.take() {
                        record(self.started_at.elapsed());
                    }
                }
            }
            let _telemetry_guard = __TelemetryDurationGuard {
                started_at: std::time::Instant::now(),
                record: Some(__telemetry_record),
            };
            #block
        }
    })
}

fn expand_http_request_function(attr: TokenStream2, item: TokenStream2) -> Result<TokenStream2> {
    let args = syn::parse2::<MeasureHttpRequestArgs>(attr)?;
    let metrics = syn::parse_str::<Expr>(&args.metrics.value())?;
    let request = syn::parse_str::<Ident>(&args.request.value())?;
    let route = syn::parse_str::<Expr>(&args.route.value())?;
    let function = syn::parse2::<ItemFn>(item)?;
    let attrs = &function.attrs;
    let vis = &function.vis;
    let sig = &function.sig;
    let block = &function.block;

    Ok(quote! {
        #(#attrs)*
        #vis #sig {
            let __telemetry_metrics = &#metrics;
            __telemetry_metrics.#request();
            __telemetry_metrics.add_http_inflight_requests(#route, 1);
            let __telemetry_record = |__telemetry_elapsed| {
                __telemetry_metrics.add_http_inflight_requests(#route, -1);
                __telemetry_metrics.record_http_request_duration(#route, __telemetry_elapsed);
            };
            struct __TelemetryHttpRequestGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                started_at: std::time::Instant,
                record: Option<F>,
            }
            impl<F> Drop for __TelemetryHttpRequestGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                fn drop(&mut self) {
                    if let Some(record) = self.record.take() {
                        record(self.started_at.elapsed());
                    }
                }
            }
            let _telemetry_guard = __TelemetryHttpRequestGuard {
                started_at: std::time::Instant::now(),
                record: Some(__telemetry_record),
            };
            #block
        }
    })
}

#[derive(Debug)]
struct MeasureDurationArgs {
    metrics: LitStr,
    record: LitStr,
}

impl Parse for MeasureDurationArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut metrics = None;
        let mut record = None;

        while !input.is_empty() {
            let key = input.parse::<Ident>()?;
            input.parse::<Token![=]>()?;
            let value = input.parse::<LitStr>()?;
            match key.to_string().as_str() {
                "metrics" => assign_lit_str(&mut metrics, value, "metrics")?,
                "record" => assign_lit_str(&mut record, value, "record")?,
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "unsupported telemetry argument",
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self {
            metrics: required_lit_str(metrics, "metrics")?,
            record: required_lit_str(record, "record")?,
        })
    }
}

#[derive(Debug)]
struct MeasureHttpRequestArgs {
    metrics: LitStr,
    request: LitStr,
    route: LitStr,
}

impl Parse for MeasureHttpRequestArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut metrics = None;
        let mut request = None;
        let mut route = None;

        while !input.is_empty() {
            let key = input.parse::<Ident>()?;
            input.parse::<Token![=]>()?;
            let value = input.parse::<LitStr>()?;
            match key.to_string().as_str() {
                "metrics" => assign_lit_str(&mut metrics, value, "metrics")?,
                "request" => assign_lit_str(&mut request, value, "request")?,
                "route" => assign_lit_str(&mut route, value, "route")?,
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "unsupported telemetry argument",
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self {
            metrics: required_lit_str(metrics, "metrics")?,
            request: required_lit_str(request, "request")?,
            route: required_lit_str(route, "route")?,
        })
    }
}

fn assign_lit_str(slot: &mut Option<LitStr>, value: LitStr, key: &str) -> Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new(
            value.span(),
            format!("duplicate `{key}` argument"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn required_lit_str(value: Option<LitStr>, key: &str) -> Result<LitStr> {
    value.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("missing `{key}` argument"),
        )
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::{expand_http_request_function, expand_measured_function};

    #[test]
    fn parse_attribute_arguments_accepts_any_order() {
        let parsed_args = syn::parse_str::<super::MeasureDurationArgs>(
            r#"record = "record_metric", metrics = "state.metrics""#,
        );
        assert!(
            parsed_args.is_ok(),
            "attribute arguments should parse: {parsed_args:?}"
        );
        let Some(args) = parsed_args.ok() else {
            return;
        };

        assert_eq!(args.metrics.value(), "state.metrics");
        assert_eq!(args.record.value(), "record_metric");
    }

    #[test]
    fn expand_async_function_records_duration_after_body() {
        let expanded = expand_measured_function(
            quote!(metrics = "state.metrics", record = "record_metric"),
            quote! {
                async fn measured() -> Option<()> {
                    return Some(());
                }
            },
        );
        assert!(
            expanded.is_ok(),
            "attribute expansion should succeed: {expanded:?}"
        );
        let Some(expanded) = expanded.ok() else {
            return;
        };
        let expanded = expanded.to_string();

        assert!(expanded.contains("struct __TelemetryDurationGuard"));
        assert!(expanded.contains("__telemetry_metrics . record_metric"));
    }

    #[test]
    fn expand_http_request_function_records_route_metrics() {
        let expanded = expand_http_request_function(
            quote!(
                metrics = "state.metrics",
                request = "record_http_noop_request",
                route = "HttpRoute::Noop"
            ),
            quote! {
                async fn measured() -> &'static str {
                    "ok"
                }
            },
        );
        assert!(
            expanded.is_ok(),
            "HTTP attribute expansion should succeed: {expanded:?}"
        );
        let Some(expanded) = expanded.ok() else {
            return;
        };
        let expanded = expanded.to_string();

        assert!(expanded.contains("record_http_noop_request"));
        assert!(expanded.contains("add_http_inflight_requests (HttpRoute :: Noop , 1)"));
        assert!(expanded.contains("record_http_request_duration"));
    }
}

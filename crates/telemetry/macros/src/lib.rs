//! these macros keep repetitive timing and HTTP metric boilerplate out of
//! runtime handlers while leaving metric ownership in normal rust values
//! each macro expands at the annotated function boundary and records through
//! expressions supplied by the caller
//!
//! attribute syntax:
//!
//! ```rust,ignore
//! #[measure_duration(metrics = "metrics", record = "record_operation")]
//! #[measure_http_request(
//!     metrics = "metrics",
//!     request = "record_http_noop_request",
//!     route = "HttpRoute::Noop",
//! )]
//! ```
//!
//! example:
//!
//! ```rust,ignore
//! #[measure_duration(metrics = "metrics", record = "record_operation")]
//! async fn handle_request() -> Result<(), Error> {
//!     do_work().await
//! }
//! ```
//!
//! expands roughly to:
//!
//! ```rust,ignore
//! async fn handle_request() -> Result<(), Error> {
//!     let __telemetry_metrics = &metrics;
//!     let __telemetry_record = |__telemetry_elapsed| {
//!         __telemetry_metrics.record_operation(__telemetry_elapsed);
//!     };
//!     struct __TelemetryGuard<F>
//!     where
//!         F: FnOnce(std::time::Duration),
//!     {
//!         started_at: std::time::Instant,
//!         record: Option<F>,
//!     }
//!     impl<F> Drop for __TelemetryGuard<F>
//!     where
//!         F: FnOnce(std::time::Duration),
//!     {
//!         fn drop(&mut self) {
//!             if let Some(record) = self.record.take() {
//!                 record(self.started_at.elapsed());
//!             }
//!         }
//!     }
//!     let _telemetry_guard = __TelemetryGuard {
//!         started_at: std::time::Instant::now(),
//!         record: Some(__telemetry_record),
//!     };
//!     do_work().await
//! }
//! ```
//!
//! the macros are optional behind the `o-sfu-telemetry` `macros` feature
//! projects that prefer explicit timing calls can use the telemetry crate
//! without enabling this proc-macro crate

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
    let mut args = syn::parse2::<TelemetryArgs>(attr)?;
    let metrics = args.required_expr("metrics")?;
    let record = args.required_ident("record")?;
    args.finish()?;

    expand_guarded_function(
        item,
        &quote! {
            let __telemetry_metrics = &#metrics;
        },
        &quote! {
            __telemetry_metrics.#record(__telemetry_elapsed);
        },
    )
}

fn expand_http_request_function(attr: TokenStream2, item: TokenStream2) -> Result<TokenStream2> {
    let mut args = syn::parse2::<TelemetryArgs>(attr)?;
    let metrics = args.required_expr("metrics")?;
    let request = args.required_ident("request")?;
    let route = args.required_expr("route")?;
    args.finish()?;

    expand_guarded_function(
        item,
        &quote! {
            let __telemetry_metrics = &#metrics;
            __telemetry_metrics.#request();
            __telemetry_metrics.add_http_inflight_requests(#route, 1);
        },
        &quote! {
            __telemetry_metrics.add_http_inflight_requests(#route, -1);
            __telemetry_metrics.record_http_request_duration(#route, __telemetry_elapsed);
        },
    )
}

fn expand_guarded_function(
    item: TokenStream2,
    setup: &TokenStream2,
    on_drop: &TokenStream2,
) -> Result<TokenStream2> {
    let function = syn::parse2::<ItemFn>(item)?;
    let attrs = &function.attrs;
    let vis = &function.vis;
    let sig = &function.sig;
    let block = &function.block;

    Ok(quote! {
        #(#attrs)*
        #vis #sig {
            #setup
            let __telemetry_record = |__telemetry_elapsed| {
                #on_drop
            };
            struct __TelemetryGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                started_at: std::time::Instant,
                record: Option<F>,
            }
            impl<F> Drop for __TelemetryGuard<F>
            where
                F: FnOnce(std::time::Duration),
            {
                fn drop(&mut self) {
                    if let Some(record) = self.record.take() {
                        record(self.started_at.elapsed());
                    }
                }
            }
            let _telemetry_guard = __TelemetryGuard {
                started_at: std::time::Instant::now(),
                record: Some(__telemetry_record),
            };
            #block
        }
    })
}

struct TelemetryArgs {
    values: Vec<(Ident, LitStr)>,
}

impl Parse for TelemetryArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut values = Vec::new();

        while !input.is_empty() {
            let key = input.parse::<Ident>()?;
            input.parse::<Token![=]>()?;
            let value = input.parse::<LitStr>()?;

            if values.iter().any(|(existing, _)| existing == &key) {
                return Err(syn::Error::new(
                    value.span(),
                    format!("duplicate `{key}` argument"),
                ));
            }

            values.push((key, value));
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(Self { values })
    }
}

impl TelemetryArgs {
    fn required_expr(&mut self, key: &str) -> Result<Expr> {
        syn::parse_str::<Expr>(&self.take(key)?.value())
    }

    fn required_ident(&mut self, key: &str) -> Result<Ident> {
        syn::parse_str::<Ident>(&self.take(key)?.value())
    }

    fn take(&mut self, key: &str) -> Result<LitStr> {
        let Some(position) = self
            .values
            .iter()
            .position(|(candidate, _)| candidate == key)
        else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("missing `{key}` argument"),
            ));
        };

        Ok(self.values.swap_remove(position).1)
    }

    fn finish(self) -> Result<()> {
        if let Some((key, _)) = self.values.first() {
            return Err(syn::Error::new(
                key.span(),
                "unsupported telemetry argument",
            ));
        }
        Ok(())
    }
}

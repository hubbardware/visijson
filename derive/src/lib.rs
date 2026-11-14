//! `#[derive(Document)]`: generate `visijson::typed::Document` for a
//! named-field struct — `to_node` builds the object tree, `diff_into`
//! compares field-by-field against a previous value at native speed and
//! emits ops only for fields that changed. Fields must themselves implement
//! `Document` (primitives, `String`, `Option`, `Vec`, string-keyed maps,
//! and other derived structs all do).

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(Document)]
pub fn derive_document(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let fields = match &ast.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return syn::Error::new_spanned(
                    &ast.ident,
                    "#[derive(Document)] requires named fields",
                )
                .to_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(&ast.ident, "#[derive(Document)] only supports structs")
                .to_compile_error()
                .into()
        }
    };

    let inserts = fields.iter().map(|f| {
        let ident = f.ident.as_ref().unwrap();
        let key = ident.to_string();
        quote! {
            map.insert(
                #key.to_string(),
                ::visijson::typed::Document::to_node(&self.#ident),
            );
        }
    });
    let diffs = fields.iter().map(|f| {
        let ident = f.ident.as_ref().unwrap();
        let key = ident.to_string();
        quote! {
            path.push(#key);
            ::visijson::typed::Document::diff_into(
                &self.#ident,
                &old.#ident,
                path,
                ops,
            );
            path.pop();
        }
    });

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    quote! {
        impl #impl_generics ::visijson::typed::Document for #name #ty_generics #where_clause {
            fn to_node(&self) -> ::std::sync::Arc<::visijson::model::Node> {
                let mut map = ::visijson::typed::OrdMap::new();
                #(#inserts)*
                ::std::sync::Arc::new(::visijson::model::Node::Object(map))
            }
            fn diff_into<'doc>(
                &'doc self,
                old: &'doc Self,
                path: &mut ::visijson::typed::DiffPath<'doc>,
                ops: &mut ::std::vec::Vec<::visijson::store::Op>,
            ) {
                #(#diffs)*
            }
        }
    }
    .into()
}

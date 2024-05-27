extern crate proc_macro;

use crate::proc_macro::TokenStream;

use quote::quote;

use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(IsSimulationEvent)]
pub fn sim_event_derive(input: TokenStream) -> TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  let name = &input.ident;

  TokenStream::from(quote! {
    impl dslab_kubernetriks::core::common::SimulationEvent for #name { }
  })
}

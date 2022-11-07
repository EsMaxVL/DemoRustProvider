//! Implements `BasicDataProvider`

use crate::error::dispatch_web3_errors;
use crate::{sync_retry, DataProvider, DataProviderConfig};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use bs_common::EvmChain;
use digest::Digest;
use uuid::Uuid;
use web3::transports::Http;
use web3::types::{BlockId, BlockNumber, CallRequest};
use web3::Web3;

#[derive(Clone)]
pub struct BasicDataProvider {
    web3_clients: HashMap<EvmChain, Web3<Http>>,
    external_blocks: Arc<RwLock<BTreeMap<EvmChain, u64>>>,
}

impl BasicDataProvider {
    pub fn new(config: &DataProviderConfig, _epoch: u64, _strategy_id: Uuid) -> Self {
        Self {
            web3_clients: config.web3_clients.clone(),
            external_blocks: Default::default(),
        }
    }

    pub fn set_epoch(&self, _epoch: u64) {}

    pub fn set_external_blocks(&self, blocks: BTreeMap<EvmChain, u64>) {
        *self
            .external_blocks
            .try_write()
            .expect("External blocks lock-write failed") = blocks;
    }

    pub fn get_chain_node_and_block(
        &self,
        chain: &bs_common::EvmChain,
    ) -> (&Web3<Http>, u64) {
        let node = self
            .web3_clients
            .get(chain)
            .expect("Not all chain nodes provided");
        let block = *self
            .external_blocks
            .read()
            .expect("External blocks lock-read failed")
            .get(chain)
            .expect("Not all block numbers provided");
        (node, block)
    }

    pub fn compute_hash<D: Digest>(&self, _digest: &mut D) {}
}

impl DataProvider for BasicDataProvider {
    fn call_evm_view(
        &self,
        chain: bs_common::EvmChain,
        address: web3::types::Address,
        calldata: &[u8],
    ) -> Result<bs_common::DataProviderResponse<Vec<u8>>, crate::error::DataProviderError>
    {
        let (chain_node, block) = self.get_chain_node_and_block(&chain);
        let res = sync_retry(
            move || {
                let cr = CallRequest::builder()
                    .to(address)
                    .data(web3::types::Bytes(calldata.to_vec()))
                    .build();
                dispatch_web3_errors(web3::block_on(
                    chain_node
                        .eth()
                        .call(cr, Some(BlockId::Number(BlockNumber::Number(block.into())))),
                ))
            },
            3000,
        )
        .map(|x| x.0);
        Ok(res)
    }

    fn evm_block_number(
        &self,
        chain: bs_common::EvmChain,
    ) -> Result<u64, crate::error::DataProviderError> {
        let (_, block) = self.get_chain_node_and_block(&chain);
        let res = block;
        Ok(res)
    }

    fn evm_account_balance(
        &self,
        chain: bs_common::EvmChain,
        address: web3::types::Address,
    ) -> Result<
        bs_common::DataProviderResponse<web3::types::U256>,
        crate::error::DataProviderError,
    > {
        let (chain_node, block) = self.get_chain_node_and_block(&chain);
        let res = sync_retry(
            move || {
                dispatch_web3_errors(web3::block_on(
                    chain_node
                        .eth()
                        .balance(address, Some(BlockNumber::Number(block.into()))),
                ))
            },
            3000,
        );
        Ok(res)
    }
}

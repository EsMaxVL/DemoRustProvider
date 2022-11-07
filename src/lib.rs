//! Implements basic, snapshot, and test data providers.
//!
//! **Warning**
//!
//! This crate is still in rapid development and everything may change until
//! the DeProtocol mainnet is launched.
//! You bear all the risks while using the library in its current state.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unused)]
#![deny(unreachable_code)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(dead_code)]
#![feature(vec_into_raw_parts)]

//mod basic_dataprovider;
/// Data provider-related errors
pub mod error;
mod snapshot_dataprovider;
//pub mod testdataprovider;

//pub use basic_dataprovider::BasicDataProvider;
use crate::error::DataProviderError;
use bs_common::{
    solana::programaccountfilter::SolanaRpcFilterType, DataProviderResponse, EvmChain, SolanaChain,
};
pub use snapshot_dataprovider::SnapshotDataProvider;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use web3::{transports::Http, Web3};

/// Functionality of a data provider.
/// Defines basic operations on Solana and EVM-compatible chains.
#[async_trait::async_trait]
pub trait DataProvider {
    /// Executes a basic (read-only) message call on an EVM-compatible chain.
    fn call_evm_view(
        &self,
        chain: EvmChain,
        address: web3::types::Address,
        calldata: &[u8],
    ) -> Result<DataProviderResponse<Vec<u8>>, DataProviderError>;

    /// Retrieve block number of an EVM-compatible chain.
    fn evm_block_number(&self, chain: EvmChain) -> Result<u64, DataProviderError>;

    /// Retrieve the account balance of an EVM-compatible chain.
    fn evm_account_balance(
        &self,
        chain: EvmChain,
        address: web3::types::Address,
    ) -> Result<DataProviderResponse<web3::types::U256>, DataProviderError>;

    /// Retrieve a Solana account information.
    fn solana_account_data(
        &self,
        chain: SolanaChain,
        address: Pubkey,
    ) -> Result<DataProviderResponse<Vec<u8>>, DataProviderError>;

    /// Retrieve the Solana block number.
    fn solana_block_number(&self, chain: SolanaChain) -> Result<u64, DataProviderError>;

    /// Retrieve the balance of a Solana account.
    fn solana_account_balance(
        &self,
        chain: SolanaChain,
        address: Pubkey,
    ) -> Result<DataProviderResponse<u64>, DataProviderError>;

    /// Returns all accounts owned by the provided program pubkey.
    fn solana_program_accounts(
        &self,
        chain: SolanaChain,
        address: Pubkey,
        filters: &[SolanaRpcFilterType],
    ) -> Result<DataProviderResponse<Vec<(Pubkey, solana_sdk::account::Account)>>, DataProviderError>;
}

// TODO: make config specific per component (e.g. in-memory data provider dont need data path)
/// Configuration of nodes in Solana and EVM-compatible chains
#[derive(Clone)]
pub struct DataProviderConfig {
    eth_clients: HashMap<EvmChain, Web3<Http>>,
    solana_clients: HashMap<SolanaChain, Arc<solana_client::rpc_client::RpcClient>>,
    data_path: PathBuf,
}

impl DataProviderConfig {
    /// Creates a new `DataProviderConfig` instance.
    pub fn new<P: AsRef<Path>>(
        data_path: P,
        eth_nodes: &HashMap<EvmChain, String>,
        solana_nodes: &HashMap<SolanaChain, String>,
    ) -> Self {
        let eth_clients: HashMap<EvmChain, Web3<Http>> = eth_nodes
            .iter()
            .map(|(chain, url)| {
                (
                    *chain,
                    Web3::new(Http::new(url).expect("Invalid web3 node url")),
                )
            })
            .collect();
        let solana_clients: HashMap<SolanaChain, _> = solana_nodes
            .iter()
            .map(|(chain, url)| {
                (
                    *chain,
                    Arc::new(RpcClient::new_with_timeout_and_commitment(
                        url,
                        Duration::from_secs(20),
                        CommitmentConfig::confirmed(),
                    )),
                )
            })
            .collect();
        Self {
            data_path: data_path.as_ref().to_owned(),
            eth_clients,
            solana_clients,
        }
    }

    /// Load current blocks for all Ethereum-like networks defined in config
    pub async fn load_eth_blocks(&self) -> anyhow::Result<BTreeMap<EvmChain, u64>> {
        let mut blocks = BTreeMap::new();
        for (chain, client) in self.eth_clients.iter() {
            let block = client.eth().block_number().await?;
            blocks.insert(*chain, block.as_u64());
        }
        Ok(blocks)
    }

    /// Load current blocks for all Solana-like networks defined in config
    pub fn load_solana_blocks(&self) -> anyhow::Result<BTreeMap<SolanaChain, u64>> {
        let mut blocks = BTreeMap::new();
        for (chain, client) in self.solana_clients.iter() {
            let block = client.get_block_height_with_commitment(CommitmentConfig::confirmed())?;
            blocks.insert(*chain, block);
        }
        Ok(blocks)
    }

    /// Returns all Ethereum-like networks defined in config
    pub fn get_evm_chainlist(&self) -> Vec<EvmChain> {
        self.eth_clients.keys().cloned().collect()
    }

    /// Returns all Solana-like networks defined in config
    pub fn get_solana_chainlist(&self) -> Vec<SolanaChain> {
        self.solana_clients.keys().cloned().collect()
    }

    /// Returns web3 client for corresponding chain
    pub fn get_eth_client(&self, chain: &EvmChain) -> Option<Web3<Http>> {
        self.eth_clients.get(chain).cloned()
    }

    /// Returns Solana RPC client for corresponding chain
    pub fn get_solana_client(&self, chain: &SolanaChain) -> Option<Arc<RpcClient>> {
        self.solana_clients.get(chain).cloned()
    }
}

/// Common interface over block oracles
#[async_trait::async_trait]
pub trait BlockProvider {
    /// Return current block in Ethereum-like chain
    async fn get_eth_block(&self, chain: EvmChain) -> Option<u64>;
    /// Return current block in Solana-like chain
    async fn get_solana_block(&self, chain: SolanaChain) -> Option<u64>;
}

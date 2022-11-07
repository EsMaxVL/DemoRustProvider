//! Implements `SnapshotProvider`

use crate::error::{dispatch_solana_errors, dispatch_web3_errors, DataProviderError};
use crate::{BlockProvider, DataProvider, DataProviderConfig};
use bs_common::solana::programaccountfilter::SolanaRpcFilterType;
use bs_common::{
    deserialize_from_bytes, serialize_to_bytes, AllChains, DataReadError, EvmChain, SolanaChain,
};
use digest::Digest;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use web3::ethabi::Address;
use web3::transports::Http;
use web3::types::{BlockId, BlockNumber, CallRequest};
use web3::Web3;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SnapshotType {
    EvmCall(Address),
    EvmAccountBalance(Address),
    EvmBlockNumber,
    SolanaAccountData(Pubkey),
    SolanaAccountBalance(Pubkey),
    SolanaBlockNumber,
    SolanaProgramAccounts(Pubkey, Vec<u8>),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct SnapshotKey {
    epoch: u64,
    chain: AllChains,
    t: SnapshotType,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    data: BTreeMap<SnapshotKey, Vec<u8>>,
    path: PathBuf,
    epoch: u64,
}

impl Snapshot {
    pub fn load_or_create<P: AsRef<Path>>(path: P, epoch: u64) -> Self {
        if path.as_ref().is_file() {
            Self {
                data: deserialize_from_bytes(
                    &std::fs::read(&path).expect("Failed to read snapshot"),
                )
                .expect("Failed to parse snapshot"),
                path: path.as_ref().to_owned(),
                epoch,
            }
        } else {
            Self {
                data: Default::default(),
                path: path.as_ref().to_owned(),
                epoch,
            }
        }
    }

    pub fn get(&self, key: &SnapshotKey) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: SnapshotKey, data: Vec<u8>) {
        self.data.insert(key, data);
        std::fs::write(&self.path, &serialize_to_bytes(&self.data).unwrap())
            .expect("snapshot commit failed");
    }
}

/// Configuration information of snapshot data providers
#[derive(Clone)]
pub struct SnapshotDataProvider<BP>
where
    BP: BlockProvider + Clone,
{
    eth_clients: Arc<HashMap<EvmChain, Web3<Http>>>,
    solana_clients: Arc<HashMap<SolanaChain, Arc<RpcClient>>>,
    block_provider: BP,
    snapshot: Arc<RwLock<Snapshot>>,
}

impl<BP> SnapshotDataProvider<BP>
where
    BP: BlockProvider + Clone,
{
    /// SnapshotDataProvider constructor
    pub fn new(config: &DataProviderConfig, epoch: u64, block_provider: BP) -> Self {
        let snapshot_path = config.data_path.join("dataprovider_snapshot");
        log::info!("Snapshot @ {}", snapshot_path.display());
        let snapshot = Snapshot::load_or_create(&snapshot_path, epoch);
        Self {
            eth_clients: Arc::new(config.eth_clients.clone()),
            solana_clients: Arc::new(config.solana_clients.clone()),
            block_provider,
            snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }

    /// Set the epoch (time required for the application to grow by `k` blocks).
    pub fn set_epoch(&self, epoch: u64) {
        self.snapshot
            .try_write()
            .expect("Snapshot lock-write failed")
            .epoch = epoch;
    }

    /// Get eth-like network node and current block
    pub async fn get_eth_chain_node_and_block(
        &self,
        chain: EvmChain,
    ) -> (&Web3<Http>, Option<u64>) {
        let node = self
            .eth_clients
            .get(&chain)
            .expect("Not all chain nodes provided");
        let block = self.block_provider.get_eth_block(chain).await;
        (node, block)
    }

    /// Get Solana node and current block
    pub async fn get_solana_chain_node_and_block(
        &self,
        chain: SolanaChain,
    ) -> (&RpcClient, Option<u64>) {
        let node = self
            .solana_clients
            .get(&chain)
            .expect("Not all chain nodes provided");
        let block = self.block_provider.get_solana_block(chain).await;
        (node, block)
    }

    /// Compute hash.
    pub fn compute_hash<D: Digest>(&self, _digest: &mut D) {}
}

impl<BP> DataProvider for SnapshotDataProvider<BP>
where
    BP: BlockProvider + Clone,
{
    fn call_evm_view(
        &self,
        chain: bs_common::EvmChain,
        address: web3::types::Address,
        calldata: &[u8],
    ) -> Result<bs_common::DataProviderResponse<Vec<u8>>, crate::error::DataProviderError>
    {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Evm(chain),
            t: SnapshotType::EvmCall(address),
            data: calldata.to_vec(),
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (chain_node, block) = web3::block_on(self.get_eth_chain_node_and_block(chain));
            let cr = CallRequest::builder()
                .to(address)
                .data(web3::types::Bytes(calldata.to_vec()))
                .build();
            let res = dispatch_web3_errors(web3::block_on(chain_node.eth().call(
                cr,
                Some(BlockId::Number(BlockNumber::Number(
                    block.ok_or(DataProviderError::UnsupportedEvmChain)?.into(),
                ))),
            )))?
            .map(|x| x.0);
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn evm_block_number(
        &self,
        chain: bs_common::EvmChain,
    ) -> Result<u64, crate::error::DataProviderError> {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Evm(chain),
            t: SnapshotType::EvmBlockNumber,
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (_, block) = web3::block_on(self.get_eth_chain_node_and_block(chain));
            let res = block.ok_or(DataProviderError::UnsupportedEvmChain)?;
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn evm_account_balance(
        &self,
        chain: bs_common::EvmChain,
        address: web3::types::Address,
    ) -> Result<
        bs_common::DataProviderResponse<web3::types::U256>,
        crate::error::DataProviderError,
    > {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Evm(chain),
            t: SnapshotType::EvmAccountBalance(address),
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (chain_node, block) = web3::block_on(self.get_eth_chain_node_and_block(chain));
            let res = dispatch_web3_errors(web3::block_on(chain_node.eth().balance(
                address,
                Some(BlockNumber::Number(
                    block.ok_or(DataProviderError::UnsupportedEvmChain)?.into(),
                )),
            )))?;
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn solana_account_data(
        &self,
        chain: bs_common::SolanaChain,
        address: solana_sdk::pubkey::Pubkey,
    ) -> Result<bs_common::DataProviderResponse<Vec<u8>>, crate::error::DataProviderError>
    {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Solana(chain),
            t: SnapshotType::SolanaAccountData(address),
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (chain_node, _) = web3::block_on(self.get_solana_chain_node_and_block(chain));
            let res = chain_node
                .get_account_with_commitment(&address, CommitmentConfig::confirmed())?
                .value
                .ok_or(DataReadError::AddressNotFound)
                .map(|x| x.data);
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn solana_block_number(
        &self,
        chain: bs_common::SolanaChain,
    ) -> Result<u64, crate::error::DataProviderError> {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Solana(chain),
            t: SnapshotType::SolanaBlockNumber,
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (_, block) = web3::block_on(self.get_solana_chain_node_and_block(chain));
            let res = block.ok_or(DataProviderError::UnsupportedSolanaChain)?;
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn solana_account_balance(
        &self,
        chain: bs_common::SolanaChain,
        address: solana_sdk::pubkey::Pubkey,
    ) -> Result<bs_common::DataProviderResponse<u64>, crate::error::DataProviderError> {
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Solana(chain),
            t: SnapshotType::SolanaAccountBalance(address),
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let (chain_node, _) = web3::block_on(self.get_solana_chain_node_and_block(chain));
            let res = dispatch_solana_errors(
                chain_node.get_balance_with_commitment(&address, CommitmentConfig::confirmed()),
            )?
            .map(|x| x.value);
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }

    fn solana_program_accounts(
        &self,
        chain: SolanaChain,
        address: Pubkey,
        filters: &[SolanaRpcFilterType],
    ) -> Result<
        bs_common::DataProviderResponse<Vec<(Pubkey, solana_sdk::account::Account)>>,
        DataProviderError,
    > {
        let filters_raw = serialize_to_bytes(&filters).unwrap();
        let mut snapshot = self.snapshot.write().expect("Snapshot lock-write failed");
        let s_key = SnapshotKey {
            epoch: snapshot.epoch,
            chain: AllChains::Solana(chain),
            t: SnapshotType::SolanaProgramAccounts(address, filters_raw),
            data: vec![],
        };
        if let Some(cached) = snapshot.get(&s_key) {
            Ok(deserialize_from_bytes(cached).expect("Corrupted snapshot"))
        } else {
            let filters: Vec<_> = filters
                .iter()
                .map(|filter| match filter {
                    SolanaRpcFilterType::DataSize(x) => RpcFilterType::DataSize(*x),
                    SolanaRpcFilterType::Memcmp(m) => {
                        RpcFilterType::Memcmp(Memcmp::new_base58_encoded(m.offset, &m.bytes))
                    }
                })
                .collect();
            let config = RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    commitment: Some(CommitmentConfig::confirmed()),
                    min_context_slot: None,
                },
                with_context: Some(false),
            };
            let (chain_node, _) = web3::block_on(self.get_solana_chain_node_and_block(chain));
            let res = chain_node.get_program_accounts_with_config(&address, config);
            let res = dispatch_solana_errors(res)?;
            snapshot.insert(s_key, serialize_to_bytes(&res).unwrap());
            Ok(res)
        }
    }
}

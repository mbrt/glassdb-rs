//! Arbitrary transaction API program generation.

use arbitrary::{Arbitrary, Unstructured};

use super::super::{MAX_CLIENTS, MAX_OPS_PER_CLIENT};
use super::{API_COLLECTION_SLOTS, API_KEYS, ApiAction, ApiTransaction, ApiWorkload};

const MAX_ACTIONS_PER_TX: usize = 6;

impl<'a> Arbitrary<'a> for ApiWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for client in 0..nclients {
            let owned: Vec<usize> = (0..API_KEYS)
                .filter(|key| key % nclients == client)
                .collect();
            let ntxs = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut txs = Vec::with_capacity(ntxs);
            for _ in 0..ntxs {
                let shape = u.arbitrary::<u8>()?;
                let actions = if shape % 4 == 0 {
                    let slot = u.arbitrary::<u8>()? as usize % API_COLLECTION_SLOTS;
                    let action = match u.arbitrary::<u8>()? % 9 {
                        0 => ApiAction::CreateCollection(slot),
                        1 => ApiAction::CreateCollectionIfAbsent(slot),
                        2 => ApiAction::ReadCollection(slot),
                        3 => ApiAction::WriteCollection(slot, u.arbitrary()?),
                        4 => ApiAction::CreateNestedCollection(slot),
                        5 => ApiAction::WriteNestedCollection(slot, u.arbitrary()?),
                        6 => ApiAction::DropNestedCollection(slot),
                        7 => ApiAction::DropCollection(slot),
                        _ => ApiAction::InspectCollections,
                    };
                    vec![action]
                } else {
                    let nactions = 1 + (shape as usize % MAX_ACTIONS_PER_TX);
                    let mut actions = Vec::with_capacity(nactions);
                    for _ in 0..nactions {
                        let key = owned[u.arbitrary::<u8>()? as usize % owned.len()];
                        actions.push(match u.arbitrary::<u8>()? % 3 {
                            0 => ApiAction::Read(key),
                            1 => ApiAction::Write(key, u.arbitrary()?),
                            _ => ApiAction::Delete(key),
                        });
                    }
                    actions
                };
                txs.push(ApiTransaction {
                    client,
                    actions,
                    abort: u.arbitrary::<u8>()? % 4 == 0,
                });
            }
            clients.push(txs);
        }
        Ok(ApiWorkload { clients })
    }
}

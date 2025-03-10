use std::collections::{HashSet, VecDeque};

use bitcoin::{Transaction, Txid};
use lender::{Lend, Lender, Lending};

use super::{MempoolTxs, MissingAncestorError, MissingDescendantError, TxInfo};

type AncestorsItem<'a> = (&'a Transaction, &'a mut TxInfo);

/// Iterator over ancestors, NOT including the specified txid, where ancestors
/// occur before descendants
pub struct AncestorsMut<'mempool_txs> {
    mempool_txs: &'mempool_txs mut MempoolTxs,
    /// `bool` indicates whether all of the tx's parents have been visited
    to_visit: Vec<(Txid, bool)>,
    visited: HashSet<Txid>,
}

impl AncestorsMut<'_> {
    fn next(
        &mut self,
    ) -> Result<Option<AncestorsItem<'_>>, MissingAncestorError> {
        let Some((txid, parents_visited)) = self.to_visit.pop() else {
            return Ok(None);
        };
        if parents_visited {
            // If this is the last item, ignore it
            if self.to_visit.is_empty() {
                return Ok(None);
            }
            let (tx, info) =
                self.mempool_txs.0.get_mut(&txid).ok_or_else(|| {
                    // FIXME: remove
                    tracing::error!("MAE next 0");
                    MissingAncestorError {
                        tx: txid,
                        missing: txid,
                    }
                })?;
            Ok(Some((tx, info)))
        } else {
            let (_, info) = self.mempool_txs.0.get(&txid).ok_or_else(|| {
                // FIXME: remove
                tracing::error!("MAE next 1");
                MissingAncestorError {
                    tx: txid,
                    missing: txid,
                }
            })?;
            self.to_visit.push((txid, true));
            self.to_visit
                .extend(info.depends.iter().copied().filter_map(|dep| {
                    if self.visited.insert(dep) {
                        Some((dep, false))
                    } else {
                        None
                    }
                }));
            self.next()
        }
    }
}

impl<'lend> Lending<'lend> for AncestorsMut<'_> {
    type Lend = Result<AncestorsItem<'lend>, MissingAncestorError>;
}

impl Lender for AncestorsMut<'_> {
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        Self::next(self).transpose()
    }
}

type DescendantsItem<'a> = (&'a Transaction, &'a mut TxInfo);

/// Iterator over descendants, including the specified txid, where ancestors
/// occur before descendants
pub struct DescendantsMut<'mempool_txs> {
    mempool_txs: &'mempool_txs mut MempoolTxs,
    to_visit: VecDeque<Txid>,
    visited: HashSet<Txid>,
}

impl DescendantsMut<'_> {
    fn next(
        &mut self,
    ) -> Result<Option<DescendantsItem<'_>>, MissingDescendantError> {
        let Some(txid) = self.to_visit.pop_front() else {
            return Ok(None);
        };
        let (tx, info) = self.mempool_txs.0.get_mut(&txid).ok_or(
            MissingDescendantError {
                tx: txid,
                missing: txid,
            },
        )?;
        self.to_visit.extend(
            info.spent_by
                .iter()
                .copied()
                .filter(|spender| self.visited.insert(*spender)),
        );
        Ok(Some((tx, info)))
    }
}

impl<'lend> Lending<'lend> for DescendantsMut<'_> {
    type Lend = Result<DescendantsItem<'lend>, MissingDescendantError>;
}

impl Lender for DescendantsMut<'_> {
    fn next(&mut self) -> Option<Lend<'_, Self>> {
        Self::next(self).transpose()
    }
}

impl MempoolTxs {
    /// Iterator over ancestors, NOT including the specified txid, where ancestors
    /// occur before descendants
    pub fn ancestors_mut(&mut self, txid: Txid) -> AncestorsMut<'_> {
        AncestorsMut {
            mempool_txs: self,
            to_visit: vec![(txid, false)],
            visited: HashSet::new(),
        }
    }

    /// Iterator over descendants, including the specified txid, where ancestors
    /// occur before descendants
    pub fn descendants_mut(&mut self, txid: Txid) -> DescendantsMut<'_> {
        DescendantsMut {
            mempool_txs: self,
            to_visit: VecDeque::from([txid]),
            visited: HashSet::new(),
        }
    }
}

use {
    crate::packet_bundle::PacketBundle,
    sha2::{Digest, Sha256},
    solana_perf::packet::{
        BytesPacketBatch, PacketBatch,
        PacketRefMut::{self, Bytes as MutBytes, Packet as MutPacket},
        PinnedPacketBatch,
    },
    solana_short_vec::decode_shortu16_len,
    solana_signature::Signature,
};

// Bundle ID derivation mirrors `derive_bundle_id_from_sanitized_transactions` for a single
// transaction bundle: sha256( base58(first_signature) ). We intentionally do NOT hash the
// entire message nor all signatures; upstream sanitized logic uses only the primary signature
// as the transaction identity. No prefix is added to keep parity with sanitized bundles.
pub fn packet_bundle_from_packet_ref(mut pkt: PacketRefMut<'_>) -> PacketBundle {
    // NB: Unset the staked node flag to prevent forwarding.
    pkt.meta_mut().set_from_staked_node(false);
    let (batch, data_slice) = match pkt {
        MutPacket(p) => {
            let data = p.data(..).unwrap_or(&[]);
            (
                PacketBatch::Pinned(PinnedPacketBatch::new(vec![p.clone()])),
                data,
            )
        }
        MutBytes(b) => {
            let data = b.data(..).unwrap_or(&[]);
            (
                PacketBatch::Bytes(BytesPacketBatch::from(vec![b.clone()])),
                data,
            )
        }
    };

    let bundle_id = parse_first_signature(data_slice)
        .map(|sig| hash_signature(&sig))
        .unwrap();

    PacketBundle { batch, bundle_id }
}

fn hash_signature(sig: &Signature) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sig.to_string());
    format!("{:x}", hasher.finalize())
}

fn parse_first_signature(data: &[u8]) -> Option<Signature> {
    if data.is_empty() {
        return None;
    }
    let (sig_count, len_len) = decode_shortu16_len(data).ok()?;
    if sig_count == 0 {
        return None;
    }
    let needed = len_len + sig_count * 64;
    if data.len() < needed {
        return None;
    }
    let start = len_len;
    let end = start + 64;
    let sig_bytes: &[u8; 64] = data[start..end].try_into().ok()?;
    Some(Signature::from(*sig_bytes))
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_hash::Hash, solana_keypair::Keypair,
        solana_perf::packet::to_packet_batches, solana_pubkey::Pubkey,
        solana_system_transaction as system_transaction,
    };

    #[test]
    fn test_bundle_id_single_tx_matches_sanitized_logic() {
        // Create a simple transfer transaction (single signature)
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = system_transaction::transfer(&from, &to, 1, Hash::default());

        // Build packet batch containing this transaction
        let mut batches = to_packet_batches(&[tx.clone()], 1);
        assert_eq!(batches.len(), 1);
        let mut batch = batches.remove(0);
        assert_eq!(batch.len(), 1);

        // Derive bundle from packet
        let pkt = batch.iter_mut().next().expect("packet");
        let bundle = packet_bundle_from_packet_ref(pkt);

        // Expected id = sha256(base58(first signature))
        let mut hasher = Sha256::new();
        hasher.update(tx.signatures[0].to_string());
        let expected = format!("{:x}", hasher.finalize());
        assert_eq!(bundle.bundle_id, expected);
    }
}
